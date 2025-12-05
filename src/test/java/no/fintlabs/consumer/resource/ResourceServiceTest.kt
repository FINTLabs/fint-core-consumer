package no.fintlabs.consumer.resource;

import com.fasterxml.jackson.databind.ObjectMapper
import io.mockk.every
import io.mockk.mockk
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.elev.ElevResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
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
import org.junit.jupiter.api.*
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals

const val RESOURCE_NAME = "elev"
const val ORG_ID = "test.org"
const val RELATION_NAME = "elevforhold"

class ResourceServiceTest {
    private lateinit var linkGenerator: LinkGenerator
    private lateinit var consumerConfiguration: ConsumerConfiguration
    private lateinit var resourceContext: ResourceContext
    private lateinit var resourceService: ResourceService
    private lateinit var cacheService: CacheService
    private lateinit var nestedLinkMapper: NestedLinkMapper

    @BeforeEach
    fun setUp() {
        resourceContext = mockk(relaxed = true)
        consumerConfiguration = mockk(relaxed = true)
        linkGenerator = LinkGenerator(consumerConfiguration, resourceContext)
        nestedLinkMapper = mockk(relaxed = true)

        every { resourceContext.resourceNames } returns setOf(RESOURCE_NAME)
        every { resourceContext.getResource(RESOURCE_NAME) } returns FintResourceInformation(RESOURCE_NAME, ElevResource::class.java, null, false, null, null, null, null)
        every { resourceContext.relationExists(any(), any()) } returns true
        every { resourceContext.isNotFintReference(any(), any()) } returns true
        every { resourceContext.getRelationUri(any(), any()) } returns "utdanning/elev/elevforhold"
        every { consumerConfiguration.orgId } returns ORG_ID
        every { consumerConfiguration.baseUrl } returns "https://test.felleskomponent.no"
        every { consumerConfiguration.componentUrl } returns "https://test.felleskomponent.no/utdanning/elev/elevforhold"
        every { nestedLinkMapper.packageToUriMap } returns mapOf()
        cacheService = CacheService()

        val nestedLinkService = NestedLinkService(consumerConfiguration, nestedLinkMapper, LinkParser())
        val linkService = LinkService(mockk(relaxed = true), linkGenerator, nestedLinkService, resourceContext)
        val relationService = mockk<RelationService>(relaxed = true);
        val resourceMapper = ResourceMapperService(ObjectMapper(), resourceContext);
        val relationRequestProducer = mockk<RelationRequestProducer>();
        val consumerConfiguration = mockk<ConsumerConfiguration>();
        val syncTrackerService = mockk<SyncTrackerService>(relaxed = true);
        resourceService = ResourceService(linkService, cacheService, relationService, resourceMapper, relationRequestProducer, consumerConfiguration, syncTrackerService)
    }

    @Test
    fun `ensure lastDelivered is set upon new resource`() {
        // Given
        val resourceId = UUID.randomUUID().toString()
        val oneDayAgo = System.currentTimeMillis() - Duration.ofDays(1).toMillis()
        val entityConsumerRecord = createEntityConsumerRecord(resourceId, timestamp = oneDayAgo)

        // When
        resourceService.processEntityConsumerRecord(entityConsumerRecord)

        // Then
        assertNotNull(getResourceFromCache(resourceId))
        assertEquals(oneDayAgo, getLastUpdated())
    }

    @Test
    fun `ensure expired resources are evicted upon cache eviction`() {
        // Given
        val resourceIdA = UUID.randomUUID().toString()
        val resourceIdB = UUID.randomUUID().toString()
        val resourceIdC = UUID.randomUUID().toString()
        val resourceIdD = UUID.randomUUID().toString()

        resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceIdA, timestamp = 1))
        resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceIdB, timestamp = 2))
        resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceIdC, timestamp = 3))
        resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceIdD, timestamp = 4))

        assertEquals(4, getCache().size())
        assertNotNull(getResourceFromCache(resourceIdA))
        assertNotNull(getResourceFromCache(resourceIdB))
        assertNotNull(getResourceFromCache(resourceIdC))
        assertNotNull(getResourceFromCache(resourceIdD))

        // When
        triggerCacheEviction(3)

        // Then
        assertEquals(2, getCache().size())
        assertNotNull(getResourceFromCache(resourceIdC))
        assertNotNull(getResourceFromCache(resourceIdD))
    }

    @Test
    fun mapResourceAndLinksSuccess() {
        val elevResource: ElevResource = createElevResource("123")
        elevResource.addElevforhold(Link.with("systemid/321"))

        val fintResource: FintResource = resourceService.mapResourceAndLinks("elev", elevResource as Any)

        Assertions.assertEquals(
            "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/321",
            fintResource
                .links[RELATION_NAME]!!
                .first()
                .href,
        )
    }

    private fun triggerCacheEviction(timestamp: Long) = getCache().evictExpired(timestamp)

    private fun getResourceFromCache(resourceId: String) = getCache().get(resourceId)

    private fun getLastUpdated() = getCache().getLastUpdated()

    private fun getCache() = cacheService.getCache(RESOURCE_NAME)

    private fun createEntityConsumerRecord(
        resourceId: String,
        resource: FintResource? = createElevResource(resourceId),
        timestamp: Long = System.currentTimeMillis()
    ) = KafkaEntity(
        key = resourceId,
        resourceName = RESOURCE_NAME,
        resource = resource,
        timestamp = timestamp,
        type = SyncType.FULL,
        corrId = UUID.randomUUID().toString(),
        totalSize = 1L
    )

    private fun createElevResource(id: String?): ElevResource =
        ElevResource().apply {
            systemId =
                Identifikator().apply {
                    identifikatorverdi = id
                }
        }
}