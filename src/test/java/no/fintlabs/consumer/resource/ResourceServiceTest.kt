package no.fintlabs.consumer.resource

import com.fasterxml.jackson.databind.ObjectMapper
import io.mockk.every
import io.mockk.mockk
import no.fint.antlr.FintFilterService
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.elev.ElevResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheManager
import no.fintlabs.cache.CacheService
import no.fintlabs.cache.config.CacheConfig
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.entity.ConsumerRecordMetadata
import no.fintlabs.consumer.kafka.entity.KafkaEntity
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import no.fintlabs.consumer.kafka.sync.SyncTrackerService
import no.fintlabs.consumer.links.LinkGenerator
import no.fintlabs.consumer.links.LinkParser
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.links.nested.NestedLinkMapper
import no.fintlabs.consumer.links.nested.NestedLinkService
import no.fintlabs.consumer.links.relation.RelationService
import no.fintlabs.consumer.resource.context.ResourceContext
import no.fintlabs.consumer.resource.context.model.FintResourceInformation
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.*
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class ResourceServiceTest {
    private lateinit var linkGenerator: LinkGenerator
    private lateinit var cacheManager: CacheManager
    private lateinit var consumerConfiguration: ConsumerConfiguration
    private lateinit var resourceContext: ResourceContext
    private lateinit var resourceService: ResourceService
    private lateinit var cacheService: CacheService
    private lateinit var nestedLinkMapper: NestedLinkMapper

    private val resourceName = "elev"
    private val orgId = "test.org"
    private val relationName = "elevforhold"

    @BeforeEach
    fun setUp() {
        resourceContext = mockk(relaxed = true)
        consumerConfiguration = mockk(relaxed = true)
        cacheManager = CacheManager()
        linkGenerator = LinkGenerator(consumerConfiguration, resourceContext)
        nestedLinkMapper = mockk(relaxed = true)

        every { resourceContext.resourceNames } returns setOf(resourceName)
        every { resourceContext.getResource(resourceName) } returns
            FintResourceInformation(
                resourceName,
                ElevResource::class.java,
                null,
                false,
                null,
                null,
                null,
                null,
            )
        every { resourceContext.relationExists(any(), any()) } returns true
        every { resourceContext.isNotFintReference(any(), any()) } returns true
        every { resourceContext.getRelationUri(any(), any()) } returns "utdanning/elev/elevforhold"
        every { consumerConfiguration.orgId } returns orgId
        every { consumerConfiguration.baseUrl } returns "https://test.felleskomponent.no"
        every { consumerConfiguration.componentUrl } returns "https://test.felleskomponent.no/utdanning/elev/elevforhold"
        every { nestedLinkMapper.packageToUriMap } returns mapOf()

        cacheService = CacheService(resourceContext, consumerConfiguration, cacheManager, CacheConfig())

        val nestedLinkService = NestedLinkService(consumerConfiguration, nestedLinkMapper, LinkParser())
        val linkService = LinkService(mockk(relaxed = true), linkGenerator, nestedLinkService, resourceContext)
        val relationService = mockk<RelationService>(relaxed = true)
        val resourceConverter = ResourceConverter(ObjectMapper(), resourceContext)
        val oDataFilterService = mockk<FintFilterService>()
        val relationRequestProducer = mockk<RelationRequestProducer>()
        val syncTrackerService = mockk<SyncTrackerService>(relaxed = true)

        resourceService =
            ResourceService(
                linkService,
                cacheService,
                relationService,
                resourceConverter,
                oDataFilterService,
                relationRequestProducer,
                consumerConfiguration,
                syncTrackerService,
            )
    }

    @Test
    fun `ensure lastDelivered is set upon new resource`() {
        val resourceId = UUID.randomUUID().toString()
        val oneDayAgo = System.currentTimeMillis() - Duration.ofDays(1).toMillis()
        val kafkaEntity = createKafkaEntity(resourceId, lastModified = oneDayAgo)

        resourceService.processEntityConsumerRecord(kafkaEntity)

        assertNotNull(getResourceFromCache(resourceId))
        assertEquals(oneDayAgo, getLastDelivered(resourceId))
    }

    @Test
    fun `ensure received retention times updates cache retention and affects cache eviction`() {
        val resourceIdLongRetention = UUID.randomUUID().toString()
        val resourceIdShortRetention = UUID.randomUUID().toString()
        val kafkaEntityWithLongRetention = createKafkaEntity(resourceIdLongRetention, retentionTime = 100L)
        val kafkaEntityWithShortRetention = createKafkaEntity(resourceIdShortRetention, retentionTime = 1L)

        // Insert entity and set retention to 100 ms
        resourceService.processEntityConsumerRecord(kafkaEntityWithLongRetention)

        // The entity should not have expired yet and therefore not be evicted
        triggerCacheEviction()
        assertNotNull(getResourceFromCache(resourceIdLongRetention))

        // Insert entity and set retention to 1 ms
        resourceService.processEntityConsumerRecord(kafkaEntityWithShortRetention)

        // With retention time 1 ms for the cache, both entities shall be evictable after more than 1 ms
        await()
            .atMost(1, TimeUnit.SECONDS)
            .untilAsserted {
                triggerCacheEviction()
                assertNull(getResourceFromCache(resourceIdLongRetention))
                assertNull(getResourceFromCache(resourceIdShortRetention))
            }
    }

    @Test
    fun `ensure non-expired resource is not evicted upon cache eviction (default retention is 7 days)`() {
        val resourceId = UUID.randomUUID().toString()
        val sevenDaysInMillis = Duration.ofDays(7).toMillis()
        val kafkaEntity = createKafkaEntity(resourceId, retentionTime = sevenDaysInMillis)

        resourceService.processEntityConsumerRecord(kafkaEntity)

        assertNotNull(getResourceFromCache(resourceId))

        triggerCacheEviction()
        Thread.sleep(100)

        assertNotNull(getResourceFromCache(resourceId))
    }

    @Test
    fun mapResourceAndLinksSuccess() {
        val elevResource: ElevResource = createElevResource("123")
        elevResource.addElevforhold(Link.with("systemid/321"))

        val fintResource: FintResource = resourceService.mapResourceAndLinks("elev", elevResource as Any)

        Assertions.assertEquals(
            "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/321",
            fintResource
                .getLinks()[relationName]!!
                .first()
                .href,
        )
    }

    private fun triggerCacheEviction() = getCache().evictOldCacheObjects()

    private fun getResourceFromCache(resourceId: String) = getCache().get(resourceId)

    private fun getCache() = cacheService.getCache(resourceName)

    private fun getLastDelivered(resourceId: String) = getCache().getLastDelivered(resourceId)

    private fun createKafkaEntity(
        resourceId: String,
        resource: FintResource? = createElevResource(resourceId),
        lastModified: Long = System.currentTimeMillis(),
        retentionTime: Long? = null,
    ) = KafkaEntity(
        key = resourceId,
        resourceName = resourceName,
        resource = resource,
        lastModified = lastModified,
        consumerRecordMetadata =
            ConsumerRecordMetadata(
                type = SyncType.FULL,
                corrId = UUID.randomUUID().toString(),
                totalSize = 1L,
            ),
        retentionTime = retentionTime,
    )

    private fun createElevResource(id: String?): ElevResource =
        ElevResource().apply {
            systemId =
                Identifikator().apply {
                    identifikatorverdi = id
                }
        }
}
