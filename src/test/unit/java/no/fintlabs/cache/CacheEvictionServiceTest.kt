package no.fintlabs.cache

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.fint.model.resource.FintResource
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.cache.cacheObjects.CacheObject
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import no.fintlabs.status.models.ResourceEvictionPayload
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.scheduling.TaskScheduler
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ScheduledFuture
import java.util.function.BiConsumer
import kotlin.test.assertTrue

class CacheEvictionServiceTest {

    private lateinit var scheduler: TaskScheduler
    private lateinit var cacheService: CacheService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var relationRequestProducer: RelationRequestProducer
    private lateinit var service: CacheEvictionService

    @BeforeEach
    fun setUp() {
        scheduler = mockk(relaxed = true)
        cacheService = mockk(relaxed = true)
        consumerConfig = mockk {
            every { orgId } returns "org-123"
            every { domain } returns "utdanning"
            every { packageName } returns "no.fintlabs.demo"
        }
        relationRequestProducer = mockk(relaxed = true)

        service = CacheEvictionService(
            scheduler = scheduler,
            cacheService = cacheService,
            consumerConfig = consumerConfig,
            relationRequestProducer = relationRequestProducer
        )
    }

    private fun payloadWithinWindow(
        resource: String = "elevfravar",
        minutesAgo: Long = 1
    ) = ResourceEvictionPayload(
        domain = "ignored-domain",
        pkg = "ignored-pkg",
        resource = resource,
        org = "ignored-org",
        unixTimestamp = Instant.now().minus(Duration.ofMinutes(minutesAgo)).toEpochMilli()
    )

    private fun assertScheduledAtOffsetMinutes(captured: Instant, offsetMinutes: Long, toleranceSeconds: Long = 15) {
        val now = Instant.now()
        val expected = now.plus(Duration.ofMinutes(offsetMinutes))
        assertTrue(
            captured.isAfter(expected.minusSeconds(toleranceSeconds)) &&
                    captured.isBefore(expected.plusSeconds(toleranceSeconds)),
            "Expected schedule around $expected, but was $captured"
        )
    }

    @Test
    fun `within window - schedules, runs, and publishes after eviction`() {
        val resource = "elevfravar"
        val payload = payloadWithinWindow(resource)

        val fintCache = mockk<Cache<FintResource>>(relaxed = true)
        val cacheObj = mockk<CacheObject<ElevfravarResource>>()
        val elev = ElevfravarResource()
        every { cacheObj.unboxObject() } returns elev
        every { cacheService.getCache(resource) } returns fintCache
        every { fintCache.evictOldCacheObjects(any()) } answers {
            val cb = firstArg<BiConsumer<String, CacheObject<ElevfravarResource>>>()
            cb.accept("key-1", cacheObj)
        }

        val runnableSlot = slot<Runnable>()
        val instantSlot = slot<Instant>()
        every { scheduler.schedule(capture(runnableSlot), capture(instantSlot)) } returns mockk<ScheduledFuture<*>>(
            relaxed = true
        )

        service.triggerEviction(payload)

        assertScheduledAtOffsetMinutes(instantSlot.captured, CacheEvictionService.MINUTES_TO_WAIT_BEFORE_EVICTING)

        runnableSlot.captured.run()

        verify(exactly = 1) { relationRequestProducer.publish(any()) }
    }

    @Test
    fun `too old - does not schedule`() {
        val payload = ResourceEvictionPayload(
            domain = "d", pkg = "p", resource = "elevfravar", org = "o",
            unixTimestamp = Instant.now()
                .minus(Duration.ofMinutes(CacheEvictionService.MINUTES_TO_ACCEPT_EVICTION + 1))
                .toEpochMilli()
        )

        service.triggerEviction(payload)

        verify(exactly = 0) { scheduler.schedule(any<Runnable>(), any<Instant>()) }
        verify(exactly = 0) { relationRequestProducer.publish(any()) }
    }

    @Test
    fun `future - does not schedule`() {
        val payload = ResourceEvictionPayload(
            domain = "d", pkg = "p", resource = "elevfravar", org = "o",
            unixTimestamp = Instant.now().plus(Duration.ofMinutes(1)).toEpochMilli()
        )

        service.triggerEviction(payload)

        verify(exactly = 0) { scheduler.schedule(any<Runnable>(), any<Instant>()) }
        verify(exactly = 0) { relationRequestProducer.publish(any()) }
    }

    @Test
    fun `no cache found - scheduled task does nothing`() {
        val payload = payloadWithinWindow(resource = "elevfravar")

        every { cacheService.getCache("elevfravar") } returns null

        val runnableSlot = slot<Runnable>()
        every { scheduler.schedule(capture(runnableSlot), any<Instant>()) } returns mockk(relaxed = true)

        service.triggerEviction(payload)
        runnableSlot.captured.run()

        verify(exactly = 0) { relationRequestProducer.publish(any()) }
    }
}
