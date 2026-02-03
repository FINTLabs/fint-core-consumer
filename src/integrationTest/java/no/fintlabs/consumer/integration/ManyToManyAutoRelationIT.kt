package no.fintlabs.consumer.integration

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.KafkaConstants
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.elev.KontaktlarergruppeResource
import no.novari.fint.model.resource.utdanning.elev.UndervisningsforholdResource
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.nio.ByteBuffer
import java.time.Clock
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import kotlin.test.assertFalse
import kotlin.test.assertTrue

fun constructAutorelationEntityTopic(
    org: String,
    domainContext: String,
    resourceName: String,
) = "${org.replace(".", "-")}.$domainContext.entity.$resourceName"

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@EmbeddedKafka(
    partitions = 1,
    topics = [
        "foo-org.fint-core.entity.utdanning-elev-undervisningsforhold",
        "foo-org.fint-core.entity.utdanning-elev-kontaktlarergruppe",
        "foo-org.fint-core.event.relation-update",
    ],
)
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=autorelation-it",

        "fint.kafka.default-replicas=1",
        "fint.relation.base-url=https://test.felleskomponent.no",
        "fint.org-id=foo.org",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=elev",
        "fint.consumer.autorelation=true",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
class ManyToManyAutoRelationIT {
    @Value("\${fint.org-id}")
    private lateinit var fintOrg: String

    @Value("\${fint.consumer.domain}")
    private lateinit var fintDomain: String

    @Value("\${fint.consumer.package}")
    private lateinit var fintPackage: String

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var embeddedKafka: EmbeddedKafkaBroker

    @Autowired
    lateinit var registry: KafkaListenerEndpointRegistry

    @Autowired
    lateinit var cacheService: CacheService

    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    private lateinit var undervisningsforholdEntityTopic: String
    private lateinit var kontaktlarergruppeEntityTopic: String

    private val clock: Clock = Clock.systemUTC()

    private val groupId1 = "group-1"
    private val groupId2 = "group-2"
    private val groupId3 = "group-3"
    private val undervisningId = "und-auto-1"

    private val expectedBackLinkHref =
        "https://test.felleskomponent.no/utdanning/elev/undervisningsforhold/systemId/$undervisningId"
    private val backRelationName = "undervisningsforhold"

    @BeforeEach
    fun setUp() {
        registry.listenerContainers.forEach { container ->
            ContainerTestUtils.waitForAssignment(container, embeddedKafka.partitionsPerTopic)
        }

        val producerProps =
            KafkaTestUtils.producerProps(embeddedKafka).apply {
                this[org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
                    org.apache.kafka.common.serialization.StringSerializer::class.java
                this[org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
                    org.apache.kafka.common.serialization.StringSerializer::class.java
            }

        kafkaTemplate = KafkaTemplate(DefaultKafkaProducerFactory(producerProps))

        undervisningsforholdEntityTopic =
            constructAutorelationEntityTopic(fintOrg, "fint-core", "$fintDomain-$fintPackage-undervisningsforhold")
        kontaktlarergruppeEntityTopic =
            constructAutorelationEntityTopic(fintOrg, "fint-core", "$fintDomain-$fintPackage-kontaktlarergruppe")
    }

    @AfterEach
    fun tearDown() {
        cacheService.getCache("undervisningsforhold").evictExpired(Long.MAX_VALUE)
        cacheService.getCache("kontaktlarergruppe").evictExpired(Long.MAX_VALUE)
    }

    @Test
    fun `should update existing Kontaktlarergrupper when a new Undervisningsforhold links to them`() {
        populateCacheWithGroups()

        val undervisningResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                addKontaktlarergruppe(Link.with("systemid/$groupId2"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
                addSkole(Link.with("systemid/dummy-klasse"))
                addSkoleressurs(Link.with("systemid/dummy-skoleressurs"))
            }

        sendEntityRecord(undervisningResource, "undervisningsforhold")

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertLinkExistsOnGroup(groupId1)
            assertLinkExistsOnGroup(groupId2)
            assertLinkExistsOnGroup(groupId3)
        }
    }

    @Test
    fun `should remove relation from Kontaktlarergruppe when link is removed`() {
        populateCacheWithGroups()

        val initialResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                addKontaktlarergruppe(Link.with("systemid/$groupId2"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
            }

        sendEntityRecord(initialResource, "undervisningsforhold")

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            assertLinkExistsOnGroup(groupId2)
        }

        val updatedResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
            }

        sendEntityRecord(updatedResource, "undervisningsforhold")

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            assertLinkExistsOnGroup(groupId1)
            assertLinkExistsOnGroup(groupId3)

            val cachedGroup2 = cacheService.getCache("kontaktlarergruppe").get(groupId2)
            assertLinkWithHrefDoesNotExist(cachedGroup2, backRelationName, expectedBackLinkHref)
        }
    }

    @Test
    fun `should not update Undervisningsforhold when Kontaktlarergruppe adds a link`() {
        val uResource = createUndervisningsforholdResource(undervisningId)
        sendEntityRecord(uResource, "undervisningsforhold")

        val groupResource =
            createKontaktlarergruppe(groupId1).apply {
                addLink(backRelationName, Link.with("systemid/$undervisningId"))
            }

        sendEntityRecord(groupResource, "kontaktlarergruppe")

        await
            .pollDelay(Duration.ofMillis(500))
            .atMost(Duration.ofMillis(1000))
            .untilAsserted {
                val cachedU1 = cacheService.getCache("undervisningsforhold").get(undervisningId)
                assertNotNull(cachedU1)
                val links = cachedU1.links["kontaktlarergruppe"]
                assertTrue(
                    links.isNullOrEmpty(),
                    "Undervisningsforhold should not be updated by Kontaktlarergruppe (Inverse side)",
                )
            }
    }

    @Test
    fun `should preserve existing Undervisningsforhold links when Kontaktlarergruppe updates`() {
        populateCacheWithGroups()
        val uResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
            }

        sendEntityRecord(uResource, "undervisningsforhold")

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            assertLinkExistsOnGroup(groupId1)
        }

        val freshGroupFromAdapter = createKontaktlarergruppe(groupId1)
        sendEntityRecord(freshGroupFromAdapter, "kontaktlarergruppe")

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cachedGroup = cacheService.getCache("kontaktlarergruppe").get(groupId1)
            assertNotNull(cachedGroup)
            assertLinkWithHrefExists(cachedGroup, backRelationName, expectedBackLinkHref)
        }
    }

    private fun populateCacheWithGroups() {
        val corrId = UUID.randomUUID().toString()
        sendEntityRecord(createKontaktlarergruppe(groupId1), "kontaktlarergruppe", corrId, 3)
        sendEntityRecord(createKontaktlarergruppe(groupId2), "kontaktlarergruppe", corrId, 3)
        sendEntityRecord(createKontaktlarergruppe(groupId3), "kontaktlarergruppe", corrId, 3)

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cache = cacheService.getCache("kontaktlarergruppe")
            assertNotNull(cache.get(groupId1))
            assertNotNull(cache.get(groupId2))
            assertNotNull(cache.get(groupId3))
        }
    }

    private fun sendEntityRecord(
        resource: FintResource,
        resourceName: String,
        corrId: String = UUID.randomUUID().toString(),
        totalSize: Int = 1,
        timestamp: Long = clock.millis(),
    ) {
        val topic =
            when (resourceName) {
                "undervisningsforhold" -> undervisningsforholdEntityTopic
                "kontaktlarergruppe" -> kontaktlarergruppeEntityTopic
                else -> throw IllegalArgumentException("Unknown resourceName $resourceName")
            }

        val key =
            requireNotNull(resource.identifikators["systemId"]?.identifikatorverdi) {
                "Missing value for systemId identifikatorverdi"
            }
        val value = objectMapper.writeValueAsString(resource)
        val recordHeaders = RecordHeaders()
        recordHeaders.add(KafkaConstants.LAST_MODIFIED, ByteBuffer.allocate(8).putLong(timestamp).array())
        recordHeaders.add(RecordHeader(KafkaConstants.SYNC_TYPE, byteArrayOf(SyncType.FULL.ordinal.toByte())))
        recordHeaders.add(RecordHeader(KafkaConstants.SYNC_CORRELATION_ID, corrId.toByteArray()))
        recordHeaders.add(
            RecordHeader(
                KafkaConstants.SYNC_TOTAL_SIZE,
                ByteBuffer.allocate(Long.SIZE_BYTES).putLong(totalSize.toLong()).array(),
            ),
        )
        kafkaTemplate.send(ProducerRecord(topic, null, timestamp, key, value, recordHeaders)).get(10, TimeUnit.SECONDS)
    }

    private fun assertLinkExistsOnGroup(groupId: String) {
        val group = cacheService.getCache("kontaktlarergruppe").get(groupId)
        assertLinkWithHrefExists(group, backRelationName, expectedBackLinkHref)
    }

    private fun assertLinkWithHrefExists(
        resource: FintResource?,
        relationName: String,
        expectedHref: String,
    ) {
        assertNotNull(resource, "Resource should be present in cache")
        val links = resource.links[relationName]
        assertNotNull(links, "Relation '$relationName' should exist in cached resource")

        val match = links.any { it.href.equals(expectedHref, ignoreCase = true) }

        assertTrue(
            match,
            "Expected link '$expectedHref' was not found in relation '$relationName'. Found: ${links.map { it.href }}",
        )
    }

    private fun assertLinkWithHrefDoesNotExist(
        resource: FintResource?,
        relationName: String,
        unexpectedHref: String,
    ) {
        assertNotNull(resource, "Resource should be present in cache")
        val links = resource.links[relationName]

        if (links.isNullOrEmpty()) return

        val match = links.any { it.href.equals(unexpectedHref, ignoreCase = true) }

        assertFalse(
            match,
            "Link '$unexpectedHref' should NOT be present in relation '$relationName', but it was found.",
        )
    }

    private fun createKontaktlarergruppe(id: String) =
        KontaktlarergruppeResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
            addSkole(Link.with("systemid/dummy-skole"))
            addKlasse(Link.with("systemid/dummy-klasse"))
        }

    private fun createUndervisningsforholdResource(id: String) =
        UndervisningsforholdResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }
}
