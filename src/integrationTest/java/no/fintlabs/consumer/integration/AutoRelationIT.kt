package no.fintlabs.consumer.integration

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.benmanes.caffeine.cache.Cache
import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.autorelation.buffer.RelationKey
import no.fintlabs.autorelation.kafka.RelationUpdateProducer
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.KafkaConstants.*
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.awaitility.kotlin.await
import org.junit.jupiter.api.*
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
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.nio.ByteBuffer
import java.time.Clock
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@EmbeddedKafka(
    partitions = 1,
    topics = [
        "foo-org.fint-core.entity.utdanning-vurdering-elevfravar",
        "foo-org.fint-core.event.relation-update",
    ],
)
@ActiveProfiles("utdanning-vurdering")
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=autorelation-service-it",

        "fint.kafka.default-replicas=1",
        "fint.relation.base-url=https://test.felleskomponent.no",
        "fint.org-id=foo.org",
        "fint.consumer.org-id=foo.org",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=vurdering",
        "fint.consumer.autorelation=true",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
@Disabled
class AutoRelationIT {
    @Value("\${fint.consumer.org-id}")
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

    @Autowired
    lateinit var relationUpdateProducer: RelationUpdateProducer

    @Autowired
    lateinit var relationLinkCache: Cache<RelationKey, MutableList<Link>>

    private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    private lateinit var elevfravarEntityTopic: String

    private val clock: Clock = Clock.systemUTC()
    private val resourceName = "elevfravar"
    private val relationName = "fravarsregistrering"

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

        elevfravarEntityTopic =
            "${fintOrg.replace(".", "-")}.fint-core.entity.$fintDomain-$fintPackage-$resourceName"
    }

    @AfterEach
    fun tearDown() {
        cacheService.getCache(resourceName).evictExpired(Long.MAX_VALUE)
        relationLinkCache.invalidateAll()
    }

    @Test
    fun `applies relation update event to a cached resource`() {
        val resourceId = UUID.randomUUID().toString()
        val resource = createResource(resourceId)
        val linkToAdd = Link.with("systemid/child-1")

        sendEntityRecord(resourceId, resource)
        publishRelationUpdate(resourceId, RelationOperation.ADD, RelationBinding(relationName, linkToAdd))

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertLinkWithSuffixExists(links, "systemid/child-1")
        }
    }

    @Test
    fun `preserves cached relations when a fresh resource arrives`() {
        val resourceId = UUID.randomUUID().toString()
        val inheritedLink = Link.with("systemid/child-existing")

        val oldResource =
            createResource(resourceId).apply {
                links[relationName] = mutableListOf(inheritedLink)
            }
        sendEntityRecord(resourceId, oldResource)

        val newResource = createResource(resourceId)
        assertNull(newResource.links[relationName])

        sendEntityRecord(resourceId, newResource)

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertLinkWithSuffixExists(links, "systemid/child-existing")
        }
    }

    @Test
    fun `buffers relation update until the target resource arrives`() {
        val resourceId = UUID.randomUUID().toString()
        val storedLink = Link.with("systemid/child-pending")

        publishRelationUpdate(resourceId, RelationOperation.ADD, RelationBinding(relationName, storedLink))

        val resource = createResource(resourceId)
        sendEntityRecord(resourceId, resource)

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertLinkWithSuffixExists(links, "systemid/child-pending")
        }
    }

    @Test
    fun `removes existing relation when a delete update is received`() {
        val resourceId = UUID.randomUUID().toString()
        val linkToDelete = Link.with("systemid/child-to-delete")

        val resource =
            createResource(resourceId).apply {
                addLink(relationName, linkToDelete)
            }
        sendEntityRecord(resourceId, resource)

        publishRelationUpdate(resourceId, RelationOperation.DELETE, RelationBinding(relationName, linkToDelete))

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNull(links, "The relation key should be removed when the last link is deleted")
        }
    }

    @Test
    fun `cancels pending add when a matching delete arrives first`() {
        val resourceId = UUID.randomUUID().toString()
        val link = Link.with("systemid/cancel-me")

        publishRelationUpdate(resourceId, RelationOperation.ADD, RelationBinding(relationName, link))
        publishRelationUpdate(resourceId, RelationOperation.DELETE, RelationBinding(relationName, link))

        val resource = createResource(resourceId)
        sendEntityRecord(resourceId, resource)

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            if (links != null) {
                assertEquals(0, links.size, "Link should have been removed from buffer before application")
            }
        }
    }

    @Test
    fun `applies multiple pending links when the resource appears`() {
        val resourceId = UUID.randomUUID().toString()
        val link1 = Link.with("systemid/1")
        val link2 = Link.with("systemid/2")

        publishRelationUpdate(resourceId, RelationOperation.ADD, RelationBinding(relationName, link1))
        publishRelationUpdate(resourceId, RelationOperation.ADD, RelationBinding(relationName, link2))

        val resource = createResource(resourceId)
        sendEntityRecord(resourceId, resource)

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            assertNotNull(links)
            assertEquals(2, links.size)
            assertLinkWithSuffixExists(links, "systemid/1")
            assertLinkWithSuffixExists(links, "systemid/2")
        }
    }

    @Test
    fun `does not duplicate buffered links when the same add arrives twice`() {
        val resourceId = UUID.randomUUID().toString()
        val link = Link.with("systemid/duplicate")

        publishRelationUpdate(resourceId, RelationOperation.ADD, RelationBinding(relationName, link))
        publishRelationUpdate(resourceId, RelationOperation.ADD, RelationBinding(relationName, link))

        val resource = createResource(resourceId)
        sendEntityRecord(resourceId, resource)

        await.atMost(Duration.ofSeconds(5)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            assertNotNull(links)
            assertEquals(1, links.size)
        }
    }

    private fun createResource(id: String): ElevfravarResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }

    private fun sendEntityRecord(
        resourceId: String,
        resource: FintResource,
        timestamp: Long = clock.millis(),
    ) {
        val corrId = UUID.randomUUID().toString()
        val headers = RecordHeaders()
        headers.add(RecordHeader(LAST_MODIFIED, ByteBuffer.allocate(Long.SIZE_BYTES).putLong(timestamp).array()))
        headers.add(RecordHeader(SYNC_TYPE, byteArrayOf(SyncType.FULL.ordinal.toByte())))
        headers.add(RecordHeader(SYNC_CORRELATION_ID, corrId.toByteArray()))
        headers.add(
            RecordHeader(
                SYNC_TOTAL_SIZE,
                ByteBuffer.allocate(Long.SIZE_BYTES).putLong(1).array(),
            ),
        )

        val key =
            requireNotNull(resource.identifikators["systemId"]?.identifikatorverdi) {
                "Missing value for systemId identifikatorverdi"
            }
        val value = objectMapper.writeValueAsString(resource)
        kafkaTemplate
            .send(ProducerRecord(elevfravarEntityTopic, null, timestamp, key, value, headers))
            .get(10, TimeUnit.SECONDS)
    }

    private fun publishRelationUpdate(
        resourceId: String,
        operation: RelationOperation,
        relationBinding: RelationBinding,
    ) {
        val relationUpdate =
            RelationUpdate(
                targetEntity = EntityDescriptor("utdanning", "vurdering", resourceName),
                targetIds = listOf(resourceId),
                binding = relationBinding,
                operation = operation,
            )
        relationUpdateProducer.publishRelationUpdate(relationUpdate).get(10, TimeUnit.SECONDS)
    }

    private fun assertLinkWithSuffixExists(
        links: List<Link>,
        suffix: String,
    ) {
        val match = links.any { it.href?.endsWith(suffix, ignoreCase = true) == true }
        assertTrue(match, "Expected link ending with '$suffix' was not found. Found: ${links.map { it.href }}")
    }
}
