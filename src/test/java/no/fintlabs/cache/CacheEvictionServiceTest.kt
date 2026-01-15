package no.fintlabs.cache

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fint.model.resource.FintResource
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.model.RelationEvent
import no.fintlabs.cache.cacheObjects.CacheObject
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationEventProducer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.scheduling.TaskScheduler
import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer
import kotlin.test.assertEquals

class CacheEvictionServiceTest {
    private lateinit var scheduler: TaskScheduler
    private lateinit var cacheService: CacheService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var relationEventProducer: RelationEventProducer
    private lateinit var service: CacheEvictionService

    @BeforeEach
    fun setUp() {
        scheduler = mockk(relaxed = true)
        cacheService = mockk(relaxed = true)
        consumerConfig =
            mockk {
                every { orgId } returns "org.123"
                every { domain } returns "utdanning"
                every { packageName } returns "vurdering"
            }
        relationEventProducer = mockk(relaxed = true)

        service =
            CacheEvictionService(
                cacheService = cacheService,
                consumerConfig = consumerConfig,
                relationEventProducer = relationEventProducer,
            )
    }

    @Test
    fun `eviction does not trigger upon unknown resource`() {
        val resource = "unknown-resource"
        every { cacheService.getCache(resource) } returns null

        verify(exactly = 0) { relationEventProducer.publish(any()) }
    }

    @Test
    fun `publishes one relation request per evicted object`() {
        val resource = "elevfravar"

        val fintCache = mockk<Cache<FintResource>>(relaxed = true)

        val c1 = mockk<CacheObject<ElevfravarResource>> { every { unboxObject() } returns ElevfravarResource() }
        val c2 = mockk<CacheObject<ElevfravarResource>> { every { unboxObject() } returns ElevfravarResource() }
        val c3 = mockk<CacheObject<ElevfravarResource>> { every { unboxObject() } returns ElevfravarResource() }

        every { cacheService.getCache(resource) } returns fintCache
        every { fintCache.evictOldCacheObjects(any()) } answers {
            val cb = firstArg<BiConsumer<String, CacheObject<ElevfravarResource>>>()
            cb.accept("k1", c1)
            cb.accept("k2", c2)
            cb.accept("k3", c3)
        }

        val published = mutableListOf<RelationEvent>()
        every { relationEventProducer.publish(capture(published)) } returns CompletableFuture()

        service.evictExpired(resource)

        assertEquals(3, published.size, "Should publish one RelationEvent per evicted cache object")

        published.forEach {
            assertEquals("org.123", it.orgId)
            assertEquals("utdanning", it.sourceEntity.domainName)
            assertEquals("vurdering", it.sourceEntity.packageName)
            assertEquals(resource, it.sourceEntity.resourceName)
        }

        verify(exactly = 3) { relationEventProducer.publish(any()) }
    }
}
