package no.fintlabs.cache

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.fint.model.resource.FintResource
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.cache.cacheObjects.CacheObject
import no.fintlabs.cache.config.EvictionConfig
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import no.fintlabs.status.models.ResourceEvictionPayload
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.scheduling.TaskScheduler
import java.time.*
import java.util.concurrent.ScheduledFuture
import java.util.function.BiConsumer
import kotlin.test.*

class CacheEvictionServiceTest {

    private lateinit var scheduler: TaskScheduler
    private lateinit var cacheService: CacheService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var relationRequestProducer: RelationRequestProducer
    private lateinit var service: CacheEvictionService

    private val fixedNow: Instant = Instant.parse("2025-01-01T10:00:00Z")
    private val clock: Clock = Clock.fixed(fixedNow, ZoneOffset.UTC)

    private val evictionConfig = EvictionConfig(
        evictionDelay = Duration.ofMinutes(10),
        acceptanceWindow = Duration.ofMinutes(10)
    )

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
            evictionConfig = evictionConfig,
            clock = clock,
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
        unixTimestamp = fixedNow.minus(Duration.ofMinutes(minutesAgo)).toEpochMilli()
    )

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
        every { scheduler.schedule(capture(runnableSlot), capture(instantSlot)) } returns mockk<ScheduledFuture<*>>(relaxed = true)

        service.triggerEviction(payload)

        val expectedRunAt = fixedNow.plus(evictionConfig.evictionDelay)
        assertEquals(expectedRunAt, instantSlot.captured, "Scheduled time should equal now + evictionDelay")

        runnableSlot.captured.run()

        verify(exactly = 1) { relationRequestProducer.publish(any()) }
    }

    @Test
    fun `too old - does not schedule`() {
        val tooOldPayload = ResourceEvictionPayload(
            domain = "d", pkg = "p", resource = "elevfravar", org = "o",
            unixTimestamp = fixedNow.minus(evictionConfig.acceptanceWindow.plusMinutes(1)).toEpochMilli()
        )

        service.triggerEviction(tooOldPayload)

        verify(exactly = 0) { scheduler.schedule(any<Runnable>(), any<Instant>()) }
        verify(exactly = 0) { relationRequestProducer.publish(any()) }
    }

    @Test
    fun `future - does not schedule`() {
        val futurePayload = ResourceEvictionPayload(
            domain = "d", pkg = "p", resource = "elevfravar", org = "o",
            unixTimestamp = fixedNow.plus(Duration.ofMinutes(1)).toEpochMilli()
        )

        service.triggerEviction(futurePayload)

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
