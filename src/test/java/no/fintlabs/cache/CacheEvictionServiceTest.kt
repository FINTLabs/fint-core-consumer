package no.fintlabs.cache

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiConsumer
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CacheEvictionServiceTest {
    private lateinit var cacheService: CacheService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var relationRequestProducer: RelationRequestProducer
    private lateinit var meterRegistry: MeterRegistry
    private lateinit var service: CacheEvictionService
    private lateinit var cacheResourceLockService: CacheResourceLockService

    @BeforeEach
    fun setUp() {
        cacheService = mockk(relaxed = true)
        consumerConfig =
            mockk {
                every { orgId } returns "org-123"
                every { domain } returns "utdanning"
                every { packageName } returns "no.fintlabs.demo"
            }
        relationRequestProducer = mockk(relaxed = true)
        meterRegistry = SimpleMeterRegistry()
        cacheResourceLockService = CacheResourceLockService()

        service =
            CacheEvictionService(
                cacheService = cacheService,
                cacheResourceLockService = cacheResourceLockService,
                consumerConfig = consumerConfig,
                relationRequestProducer = relationRequestProducer,
                meterRegistry = meterRegistry,
            )
    }

    @Test
    fun `eviction does not trigger upon unknown resource`() {
        val resource = "unknown-resource"
        every { cacheService.getCache(resource) } returns null

        service.evictExpired(resource)

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

        service.evictExpired(resource)

        assertEquals(3, published.size, "Should publish one RelationRequest per evicted cache object")

        published.forEach {
            assertEquals("org-123", it.orgId)
            assertEquals("utdanning", it.type.domain)
            assertEquals("no.fintlabs.demo", it.type.pkg)
            assertEquals(resource, it.type.resource)
        }

        verify(exactly = 3) { relationRequestProducer.publish(any()) }
    }

    @Test
    fun `concurrent eviction trigger for same resource is queued and runs after current one`() {
        val resource = "elevfravar"
        val fintCache = mockk<Cache<FintResource>>(relaxed = true)
        every { cacheService.getCache(resource) } returns fintCache

        val firstRunStarted = CountDownLatch(1)
        val allowFirstRunToFinish = CountDownLatch(1)
        val evictionRuns = AtomicInteger(0)

        every { fintCache.evictOldCacheObjects(any()) } answers {
            if (evictionRuns.incrementAndGet() == 1) {
                firstRunStarted.countDown()
                allowFirstRunToFinish.await(2, TimeUnit.SECONDS)
            }
        }

        val first = CompletableFuture.runAsync { service.evictExpired(resource) }
        assertTrue(firstRunStarted.await(2, TimeUnit.SECONDS), "First eviction run did not start in time")

        val second = CompletableFuture.runAsync { service.evictExpired(resource) }

        allowFirstRunToFinish.countDown()

        first.get(2, TimeUnit.SECONDS)
        second.get(2, TimeUnit.SECONDS)

        assertEquals(2, evictionRuns.get(), "Second trigger should run after first completed")
    }
}
