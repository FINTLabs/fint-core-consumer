package no.fintlabs.consumer.health

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.mockk
import org.apache.kafka.clients.consumer.Consumer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.health.Status
import org.springframework.kafka.event.NonResponsiveConsumerEvent
import java.time.Duration
import java.time.Instant

class KafkaRuntimeHealthMonitorTest {
    @Test
    fun `should stay up during grace period for non responsive consumer`() {
        val clock = MutableClock(Instant.parse("2026-03-20T10:00:00Z"))
        val monitor =
            KafkaRuntimeHealthMonitor(
                KafkaHealthProperties(runtimeGracePeriod = Duration.ofMinutes(15)),
                clock,
                KafkaHealthMetrics(
                    SimpleMeterRegistry(),
                    clock,
                    KafkaHealthProperties(runtimeGracePeriod = Duration.ofMinutes(15)),
                ),
            )

        monitor.registerListener(KafkaListenerIds.ENTITY)
        monitor.onRecordProcessed(KafkaListenerIds.ENTITY)
        monitor.onNonResponsiveConsumer(nonResponsiveConsumerEvent(KafkaListenerIds.ENTITY))

        clock.advance(Duration.ofMinutes(14))

        assertEquals(Status.UP, monitor.health().status)
    }

    @Test
    fun `should go down after grace period for non responsive consumer`() {
        val clock = MutableClock(Instant.parse("2026-03-20T10:00:00Z"))
        val monitor =
            KafkaRuntimeHealthMonitor(
                KafkaHealthProperties(runtimeGracePeriod = Duration.ofMinutes(15)),
                clock,
                KafkaHealthMetrics(
                    SimpleMeterRegistry(),
                    clock,
                    KafkaHealthProperties(runtimeGracePeriod = Duration.ofMinutes(15)),
                ),
            )

        monitor.registerListener(KafkaListenerIds.ENTITY)
        monitor.onRecordProcessed(KafkaListenerIds.ENTITY)
        monitor.onNonResponsiveConsumer(nonResponsiveConsumerEvent(KafkaListenerIds.ENTITY))

        clock.advance(Duration.ofMinutes(16))

        assertEquals(Status.DOWN, monitor.health().status)
    }

    @Test
    fun `should recover after healthy activity resumes`() {
        val clock = MutableClock(Instant.parse("2026-03-20T10:00:00Z"))
        val monitor =
            KafkaRuntimeHealthMonitor(
                KafkaHealthProperties(runtimeGracePeriod = Duration.ofMinutes(15)),
                clock,
                KafkaHealthMetrics(
                    SimpleMeterRegistry(),
                    clock,
                    KafkaHealthProperties(runtimeGracePeriod = Duration.ofMinutes(15)),
                ),
            )

        monitor.registerListener(KafkaListenerIds.ENTITY)
        monitor.onNonResponsiveConsumer(nonResponsiveConsumerEvent(KafkaListenerIds.ENTITY))
        clock.advance(Duration.ofMinutes(5))

        monitor.onRecordProcessed(KafkaListenerIds.ENTITY)
        clock.advance(Duration.ofMinutes(20))

        assertEquals(Status.UP, monitor.health().status)
    }

    @Test
    fun `should publish runtime metrics for problem and unhealthy state`() {
        val clock = MutableClock(Instant.parse("2026-03-20T10:00:00Z"))
        val meterRegistry = SimpleMeterRegistry()
        val properties = KafkaHealthProperties(runtimeGracePeriod = Duration.ofMinutes(15))
        val monitor = KafkaRuntimeHealthMonitor(properties, clock, KafkaHealthMetrics(meterRegistry, clock, properties))

        monitor.registerListener(KafkaListenerIds.ENTITY)
        monitor.onNonResponsiveConsumer(nonResponsiveConsumerEvent(KafkaListenerIds.ENTITY))

        assertEquals(
            1.0,
            meterRegistry
                .get("fint.consumer.kafka.runtime.problem")
                .tag("listener", KafkaListenerIds.ENTITY)
                .tag("reason", "NON_RESPONSIVE")
                .counter()
                .count(),
        )
        assertEquals(
            0.0,
            meterRegistry
                .get("fint.consumer.kafka.runtime.unhealthy")
                .tag("listener", KafkaListenerIds.ENTITY)
                .gauge()
                .value(),
        )

        clock.advance(Duration.ofMinutes(16))

        assertEquals(
            1.0,
            meterRegistry
                .get("fint.consumer.kafka.runtime.unhealthy")
                .tag("listener", KafkaListenerIds.ENTITY)
                .gauge()
                .value(),
        )
        assertEquals(
            Duration.ofMinutes(16).toMillis().toDouble(),
            meterRegistry
                .get("fint.consumer.kafka.runtime.problem.duration")
                .tag("listener", KafkaListenerIds.ENTITY)
                .gauge()
                .value(),
        )
    }
}

private fun nonResponsiveConsumerEvent(listenerId: String): NonResponsiveConsumerEvent =
    NonResponsiveConsumerEvent(
        Any(),
        Any(),
        1_000L,
        listenerId,
        emptyList(),
        mockk<Consumer<Any, Any>>(relaxed = true),
    )
