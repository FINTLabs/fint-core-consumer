package no.fintlabs.cache

import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintResource
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationRequest
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.scheduling.TaskScheduler
import kotlin.test.assertEquals
import kotlin.test.assertSame

class CacheEvictionServiceTest {
    private lateinit var scheduler: TaskScheduler
    private lateinit var cacheService: CacheService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var relationRequestProducer: RelationRequestProducer
    private lateinit var cacheEvictionService: CacheEvictionService

    @BeforeEach
    fun setUp() {
        scheduler = mockk(relaxed = true)
        cacheService = mockk(relaxed = true)
        consumerConfig =
            mockk {
                every { orgId } returns "org-123"
                every { domain } returns "utdanning"
                every { packageName } returns "no.fintlabs.demo"
            }
        relationRequestProducer = mockk(relaxed = true)

        cacheEvictionService =
            CacheEvictionService(
                cacheService = cacheService,
                consumerConfig = consumerConfig,
                relationRequestProducer = relationRequestProducer,
            )
    }

    @Test
    fun `none delete relation requests published when cache eviction returns empty set`() {
        // Given
        val resourceName = "elevfravar"
        val cache = mockk<FintCache<FintResource>>()

        every { cacheService.getCache(resourceName) } returns cache
        every { cache.evictExpired(any()) } returns setOf()

        // When
        cacheEvictionService.evictExpired(resourceName, 10) // Timestamp does not matter as response is mocked

        // Then
        verify { relationRequestProducer wasNot Called }
    }

    @Test
    fun `publishes one delete relation request per evicted resource`() {
        // Given
        val resourceName = "elevfravar"
        val cache = mockk<FintCache<FintResource>>()
        val resourceA = createResource("A")
        val resourceB = createResource("B")
        val resourceC = createResource("C")

        every { cacheService.getCache(resourceName) } returns cache
        every { cache.evictExpired(any()) } returns setOf(resourceA, resourceB, resourceC)
        val capturedRelationRequests = mutableListOf<RelationRequest>()

        // When
        cacheEvictionService.evictExpired(resourceName, 10) // Timestamp does not matter as response is mocked

        // Then
        verify(exactly = 3) {
            relationRequestProducer.publish(capture(capturedRelationRequests))
        }

        assertSame(resourceA, capturedRelationRequests[0].resource)
        assertSame(resourceB, capturedRelationRequests[1].resource)
        assertSame(resourceC, capturedRelationRequests[2].resource)

        assertEquals(RelationOperation.DELETE, capturedRelationRequests[0].operation)
        assertEquals(RelationOperation.DELETE, capturedRelationRequests[1].operation)
        assertEquals(RelationOperation.DELETE, capturedRelationRequests[2].operation)
    }

    private fun createResource(id: String) =
        ElevfravarResource().apply {
            systemId =
                Identifikator().apply {
                    identifikatorverdi = id
                }
        }
}
