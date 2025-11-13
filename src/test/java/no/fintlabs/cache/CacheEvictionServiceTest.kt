package no.fintlabs.cache

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fint.model.resource.FintResource
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.model.RelationRequest
import no.fintlabs.cache.cacheObjects.CacheObject
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.scheduling.TaskScheduler
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer
import kotlin.test.assertEquals

class CacheEvictionServiceTest {
    private lateinit var scheduler: TaskScheduler
    private lateinit var cacheService: CacheService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var relationRequestProducer: RelationRequestProducer
    private lateinit var service: CacheEvictionService

    private val fixedNow: Instant = Instant.parse("2025-01-01T10:00:00Z")

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

        service =
            CacheEvictionService(
                cacheService = cacheService,
                consumerConfig = consumerConfig,
                relationRequestProducer = relationRequestProducer,
            )
    }

    @Test
    fun `eviction does not trigger upon unknown resource`() {
        val resource = "unknown-resource"
        every { cacheService.getCache(resource) } returns null

        verify(exactly = 0) { relationRequestProducer.publish(any()) }
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

        val published = mutableListOf<RelationRequest>()
        every { relationRequestProducer.publish(capture(published)) } returns CompletableFuture()

        service.triggerEviction(resource)

        assertEquals(3, published.size, "Should publish one RelationRequest per evicted cache object")

        published.forEach {
            assertEquals("org-123", it.orgId)
            assertEquals("utdanning", it.type.domain)
            assertEquals("no.fintlabs.demo", it.type.pkg)
            assertEquals(resource, it.type.resource)
        }

        verify(exactly = 3) { relationRequestProducer.publish(any()) }
    }
}
