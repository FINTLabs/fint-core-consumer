package no.fintlabs.consumer.integration

import com.fasterxml.jackson.databind.ObjectMapper
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.timeplan.FagResource
import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.awaitility.kotlin.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.HttpStatus
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import org.springframework.web.client.ResponseErrorHandler
import java.nio.ByteBuffer
import java.time.Clock
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

fun constructEntityTopic(
    org: String,
    domain: String,
    resourceName: String,
) = "${org.replace(".", "-")}.$domain.entity.$resourceName"

const val FAG_ENTITY_TOPIC = "foo-org.fint-core.entity.utdanning-timeplan-fag"

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@EmbeddedKafka(
    partitions = 1,
    topics = [FAG_ENTITY_TOPIC],
)
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=entity-cache-it",

        "fint.kafka.default-replicas=1",
        "fint.relation.base-url=https://foo.org",
        "fint.org-id=foo.org",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=timeplan",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
class FintCacheIT {
    @Value("\${fint.org-id}")
    private lateinit var fintOrg: String

    @Value("\${fint.consumer.domain}")
    private lateinit var fintDomain: String

    @Value("\${fint.consumer.package}")
    private lateinit var fintPackage: String

    @Autowired
    lateinit var rest: TestRestTemplate

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var embeddedKafka: EmbeddedKafkaBroker

    @Autowired
    lateinit var registry: KafkaListenerEndpointRegistry

    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    private lateinit var fagEntityTopic: String
    private lateinit var undervisningsgruppeEntityTopic: String
    private val clock: Clock = Clock.systemUTC()

    @BeforeEach
    fun setUp() {
        // Avoid "published before consumer assigned" races:
        registry.listenerContainers.forEach { container ->
            ContainerTestUtils.waitForAssignment(container, embeddedKafka.partitionsPerTopic)
        }

        // Prevent TestRestTemplate from throwing exceptions on 404 so we can assert status codes:
        rest.restTemplate.errorHandler = ResponseErrorHandler { false }

        // Producer for JSON strings into the embedded broker:
        val producerProps =
            KafkaTestUtils.producerProps(embeddedKafka).apply {
                this[org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
                    org.apache.kafka.common.serialization.StringSerializer::class.java
                this[org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
                    org.apache.kafka.common.serialization.StringSerializer::class.java
            }

        kafkaTemplate = KafkaTemplate(DefaultKafkaProducerFactory(producerProps))
        fagEntityTopic = constructEntityTopic(fintOrg, "fint-core", "$fintDomain-$fintPackage-fag")
        undervisningsgruppeEntityTopic =
            constructEntityTopic(fintOrg, "fint-core", "$fintDomain-$fintPackage-undervisningsgruppe")
    }

    @Test
    fun `create update delete events are reflected in cache and REST API`() {
        // 1) Empty list returned for empty FINT cache
        assertEquals(fetchAllFag().totalItems, 0, "Expected empty response when FINT cache is empty")

        // 2) Full sync with single resource -> Single entry in cache
        val fagA = updateFag("A")
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val fagResources = fetchAllFagResources()
            assertEquals(1, fagResources.size, "The cache should contain one entry")
            assertEquals(fagA, fagResources[0])
        }

        // 3) Full-sync with two other resources than previously synced -> The non-synced resource is purged and the two new are added
        val corrIdStep3 = UUID.randomUUID().toString()
        val timestamp3 = clock.millis()
        val fagB_3 = updateFag("B", timestamp = timestamp3, corrId = corrIdStep3)
        val fagC_3 = updateFag("C", timestamp = timestamp3, corrId = corrIdStep3)
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val fagResources = fetchAllFagResources()
            assertEquals(2, fagResources.size, "The cache should contain two entries")
            // Should be returned in same sequence as inserted
            assertEquals(fagB_3, fagResources[0])
            assertEquals(fagC_3, fagResources[1])
        }

        // 4) Full-sync updating two last synced resources -> The two existing resources are updated
        val corrIdStep4 = UUID.randomUUID().toString()
        val timestamp4 = clock.millis()
        val fagB_4 = updateFag("B", timestamp = timestamp4, corrId = corrIdStep4, descriptionToken = "Step-4")
        val fagC_4 = updateFag("C", timestamp = timestamp4, corrId = corrIdStep4, descriptionToken = "Step-4")
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val fagResources = fetchAllFagResources()
            assertEquals(2, fagResources.size, "The cache should contain two entries")
            // Should be updated with new description
            assertEquals(fagB_4, fagResources[0])
            assertEquals(fagC_4, fagResources[1])
        }

        // 5) Delta-sync updating one resource -> One of the two existing resources are updated
        val fagC_5 = updateFag("C", syncType = SyncType.DELTA, descriptionToken = "Step-5")
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val fagResources = fetchAllFagResources()
            assertEquals(2, fagResources.size, "The cache should contain two entries")
            // Should be updated with new description
            assertEquals(fagB_4, fagResources[0])
            assertEquals(fagC_5, fagResources[1])
        }

        // 6) Delete-sync removing one resource -> One of two existing resources removed
        deleteFag("B", FagResource::class.java)
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val fagResources = fetchAllFagResources()
            assertEquals(1, fagResources.size, "The cache should contain one entry")
            // Fag C should be updated with new description
            assertEquals(fagC_5, fagResources[0])
        }

        // 7) Delete-sync removing last resource -> Empty list returned for empty FINT cache
        deleteFag("C", FagResource::class.java)
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val fagResources = fetchAllFagResources()
            assertTrue(fagResources.isEmpty(), "The cache should be empty")
        }

        // 8) Full-sync with 10 003 resources -> 11 pages with 1000 records to fetch all records
        val corrIdStep8 = UUID.randomUUID().toString()
        val resourceCount = 10003
        for (resourceId in 0 until resourceCount) {
            updateFag(resourceId.toString(), corrId = corrIdStep8, totalSize = resourceCount)
        }
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val fagResources = fetchAllFagResources()
            assertEquals(resourceCount, fagResources.size, "The cache should contain two entries")
            for (resourceId in 0 until resourceCount) {
                val fagResource = fagResources[resourceId]
                assertEquals("systemid-fag-$resourceId", fagResource.systemId.identifikatorverdi)
                assertEquals("Fag-$resourceId", fagResource.navn)
                assertEquals("Beskrivelse fag $resourceId ", fagResource.beskrivelse)
            }
        }

        // 9) Fetch 10 003 resources as pages of 1000 -> 11 pages to fetch all records
        val fagResources = mutableListOf<FagResource>()
        val pageSize = 1000

        var uri = "/utdanning/timeplan/fag?size=$pageSize"

        var pageNumber = 0
        var previousTotalElements = -1

        while (uri.isNotBlank()) {
            val response = rest.getForEntity(uri, FintResourcesPage::class.java)

            assertEquals(HttpStatus.OK, response.statusCode, "Page $pageNumber failed")

            val resourcesPage = response.body
            assertNotNull(resourcesPage, "Response body should not be null on page $pageNumber")

            // Detect infinite loop / broken pagination
            assertTrue(
                resourcesPage.totalItems >= previousTotalElements,
                "totalElements should never decrease",
            )
            previousTotalElements = resourcesPage.totalItems

            val pageResources = resourcesPage.getResources(objectMapper, FagResource::class.java)
            fagResources.addAll(pageResources)

            // Get HAL next link and remove scheme + host + port
            val nextPageLink = resourcesPage.links["next"]
            val nextHref =
                nextPageLink
                    ?.get(0)
                    ?.href
                    ?.replace(Regex("^https?://[^:/]+(:\\d+)?"), "")
                    ?.takeIf { it.isNotBlank() }
                    ?: ""

            uri = nextHref

            pageNumber++
        }

        val expectedNumberOfPages = (resourceCount + pageSize - 1) / pageSize
        assertEquals(expectedNumberOfPages, pageNumber, "Expected 11 pages to read all resources")
        assertEquals(resourceCount, fagResources.size, "The cache should contain two entries")
        for (resourceId in 0 until resourceCount) {
            val fagResource = fagResources[resourceId]
            assertEquals("systemid-fag-$resourceId", fagResource.systemId.identifikatorverdi)
            assertEquals("Fag-$resourceId", fagResource.navn)
            assertEquals("Beskrivelse fag $resourceId ", fagResource.beskrivelse)
        }
    }

    private fun updateFag(
        id: String,
        syncType: SyncType = SyncType.FULL,
        timestamp: Long = clock.millis(),
        totalSize: Int = 1,
        corrId: String = UUID.randomUUID().toString(),
        descriptionToken: String = "",
    ): FintResource {
        val fag = createFagDto("systemid-fag-$id", "Fag-$id", "Beskrivelse fag $id $descriptionToken")
        sendKafkaEntityRecord(fag, timestamp, syncType, totalSize, corrId)
        return fag
    }

    private fun sendKafkaEntityRecord(
        resource: FintResource,
        timestamp: Long,
        syncType: SyncType,
        totalSize: Int,
        correlationId: String = UUID.randomUUID().toString(),
    ) {
        val topic =
            when (resource) {
                is FagResource -> fagEntityTopic
                else -> undervisningsgruppeEntityTopic
            }
        val key =
            requireNotNull(resource.identifikators["systemId"]?.identifikatorverdi) { "Missing value for systemId identifikatorverdi" }
        val value = objectMapper.writeValueAsString(resource)
        val recordHeaders = RecordHeaders()
        recordHeaders.add(LAST_MODIFIED, ByteBuffer.allocate(8).putLong(timestamp).array())
        recordHeaders.add(RecordHeader(SYNC_TYPE, byteArrayOf(syncType.ordinal.toByte())))
        recordHeaders.add(RecordHeader(SYNC_CORRELATION_ID, correlationId.toByteArray()))
        recordHeaders.add(
            RecordHeader(
                SYNC_TOTAL_SIZE,
                ByteBuffer.allocate(Long.SIZE_BYTES).putLong(totalSize.toLong()).array(),
            ),
        )
        kafkaTemplate.send(ProducerRecord(topic, null, timestamp, key, value, recordHeaders)).get(10, TimeUnit.SECONDS)
    }

    private fun <T> deleteFag(
        id: String,
        resourceType: Class<T>,
        timestamp: Long = clock.millis(),
        totalSize: Long = 1,
        correlationId: String = UUID.randomUUID().toString(),
    ) {
        val topic =
            when (resourceType) {
                FagResource::class.java -> fagEntityTopic
                else -> undervisningsgruppeEntityTopic
            }
        val key = "systemid-fag-$id"
        val recordHeaders = RecordHeaders()
        recordHeaders.add(LAST_MODIFIED, ByteBuffer.allocate(8).putLong(timestamp).array())
        recordHeaders.add(RecordHeader(SYNC_TYPE, byteArrayOf(SyncType.DELETE.ordinal.toByte())))
        recordHeaders.add(RecordHeader(SYNC_CORRELATION_ID, correlationId.toByteArray()))
        recordHeaders.add(
            RecordHeader(
                SYNC_TOTAL_SIZE,
                ByteBuffer.allocate(Long.SIZE_BYTES).putLong(totalSize).array(),
            ),
        )
        kafkaTemplate.send(ProducerRecord(topic, null, timestamp, key, null, recordHeaders)).get(10, TimeUnit.SECONDS)
    }

    private fun fetchAllFag(): FintResourcesPage {
        val response = rest.getForEntity("/utdanning/timeplan/fag", FintResourcesPage::class.java)
        assertEquals(response.statusCode, HttpStatus.OK)

        val resourcesPage = response.body ?: throw IllegalStateException("Response body is null")
        assertTrue { resourcesPage.embedded.entries.size == resourcesPage.size }
        return resourcesPage
    }

    private fun fetchAllFagResources(): List<FagResource> {
        val fagResponse = fetchAllFag()
        return fagResponse.getResources(objectMapper, FagResource::class.java)
    }

    private fun createFagDto(
        systemId: String,
        navn: String,
        beskrivelse: String,
    ): FagResource {
        val systemIdentifikator = Identifikator().apply { identifikatorverdi = systemId }
        val fag = FagResource()
        fag.systemId = systemIdentifikator
        fag.navn = navn
        fag.beskrivelse = beskrivelse
        fag.links["self"] = listOf(Link("https://$fintOrg/$fintDomain/$fintPackage/fag/systemid/$systemId"))
        return fag
    }
}
