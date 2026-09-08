package no.fintlabs.consumer.health

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@Component
class KafkaHealthMetrics(
    private val meterRegistry: MeterRegistry,
    private val clock: Clock,
    private val kafkaHealthProperties: KafkaHealthProperties,
) {
    private val counters = ConcurrentHashMap<String, Counter>()
    private val timers = ConcurrentHashMap<String, Timer>()
    private val bootstrapStates = ConcurrentHashMap<String, BootstrapMetricState>()
    private val runtimeStates = ConcurrentHashMap<String, RuntimeMetricState>()
    private val bootstrapAllStartNanos = AtomicLong(System.nanoTime())
    private val bootstrapAllRecorded = AtomicBoolean(false)

    fun registerBootstrapListener(listenerId: String) {
        bootstrapStates.computeIfAbsent(listenerId) {
            BootstrapMetricState(System.nanoTime()).also { state ->
                meterRegistry.gauge(
                    "fint.consumer.kafka.bootstrap.state",
                    listOf(Tag.of("listener", listenerId)),
                    state,
                ) { it.inProgress.get().toDouble() }
                meterRegistry.gauge(
                    "fint.consumer.kafka.bootstrap.partitions.pending",
                    listOf(Tag.of("listener", listenerId)),
                    state,
                ) { it.pendingPartitions.get().toDouble() }
            }
        }
    }

    fun updateBootstrapPendingPartitions(
        listenerId: String,
        pendingPartitions: Int,
    ) {
        bootstrapStates[listenerId]?.pendingPartitions?.set(pendingPartitions)
    }

    fun markBootstrapCompleted(listenerId: String) {
        bootstrapStates[listenerId]?.let { state ->
            state.pendingPartitions.set(0)
            state.inProgress.set(0)
            if (state.completed.compareAndSet(false, true)) {
                counter(
                    "fint.consumer.kafka.bootstrap.completed",
                    listOf(Tag.of("listener", listenerId)),
                ).increment()
                timer(
                    "fint.consumer.kafka.bootstrap.duration",
                    listOf(Tag.of("listener", listenerId)),
                ).record(System.nanoTime() - state.startNanos, TimeUnit.NANOSECONDS)
            }
        }
    }

    fun markBootstrapAllCompleted() {
        if (bootstrapAllRecorded.compareAndSet(false, true)) {
            counter(
                "fint.consumer.kafka.bootstrap.completed",
                listOf(Tag.of("listener", "all")),
            ).increment()
            timer(
                "fint.consumer.kafka.bootstrap.duration",
                listOf(Tag.of("listener", "all")),
            ).record(System.nanoTime() - bootstrapAllStartNanos.get(), TimeUnit.NANOSECONDS)
        }
    }

    fun recordBootstrapEndOffsetLookupFailure(listenerId: String) {
        counter(
            "fint.consumer.kafka.bootstrap.end_offset.lookup.failures",
            listOf(Tag.of("listener", listenerId)),
        ).increment()
    }

    fun registerRuntimeListener(listenerId: String) {
        runtimeStates.computeIfAbsent(listenerId) {
            RuntimeMetricState().also { state ->
                meterRegistry.gauge(
                    "fint.consumer.kafka.runtime.unhealthy",
                    listOf(Tag.of("listener", listenerId)),
                    state,
                ) {
                    if (it.isUnhealthy(
                            clock.millis(),
                            kafkaHealthProperties.runtimeGracePeriod.toMillis(),
                        )
                    ) {
                        1.0
                    } else {
                        0.0
                    }
                }
                meterRegistry.gauge(
                    "fint.consumer.kafka.runtime.problem.duration",
                    listOf(Tag.of("listener", listenerId)),
                    state,
                ) { it.problemDuration(clock.millis()).toDouble() }
            }
        }
    }

    fun markRuntimeHealthy(listenerId: String) {
        runtimeStates[listenerId]?.markHealthy(clock.millis())
    }

    fun markRuntimeProblem(
        listenerId: String,
        reason: String,
    ) {
        runtimeStates[listenerId]?.markProblem(clock.millis())
        counter(
            "fint.consumer.kafka.runtime.problem",
            listOf(Tag.of("listener", listenerId), Tag.of("reason", reason)),
        ).increment()
    }

    private fun counter(
        name: String,
        tags: List<Tag>,
    ): Counter {
        return counters.computeIfAbsent(meterKey(name, tags)) { meterRegistry.counter(name, tags) }
    }

    private fun timer(
        name: String,
        tags: List<Tag>,
    ): Timer {
        return timers.computeIfAbsent(meterKey(name, tags)) { meterRegistry.timer(name, tags) }
    }

    private fun meterKey(
        name: String,
        tags: List<Tag>,
    ): String {
        return "$name|${tags.joinToString("|") { "${it.key}=${it.value}" }}"
    }
}

private class BootstrapMetricState(
    val startNanos: Long,
) {
    val pendingPartitions = AtomicInteger(0)
    val inProgress = AtomicInteger(1)
    val completed = AtomicBoolean(false)
}

private class RuntimeMetricState {
    private val problemSince = AtomicLong(0L)

    fun markHealthy(now: Long) {
        problemSince.set(0L)
    }

    fun markProblem(now: Long) {
        problemSince.compareAndSet(0L, now)
    }

    fun problemDuration(now: Long): Long {
        return problemSince
            .get()
            .takeIf { it > 0L }
            ?.let { now - it }
            ?: 0L
    }

    fun isUnhealthy(
        now: Long,
        gracePeriodMs: Long,
    ): Boolean {
        return problemSince
            .get()
            .takeIf { it > 0L }
            ?.let { now - it >= gracePeriodMs }
            ?: false
    }
}
