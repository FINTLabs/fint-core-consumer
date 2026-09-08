package no.fintlabs.consumer.health

import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.context.event.EventListener
import org.springframework.kafka.event.ConsumerFailedToStartEvent
import org.springframework.kafka.event.ConsumerStartedEvent
import org.springframework.kafka.event.ConsumerStoppedEvent
import org.springframework.kafka.event.KafkaEvent
import org.springframework.kafka.event.ListenerContainerIdleEvent
import org.springframework.kafka.event.NonResponsiveConsumerEvent
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.stereotype.Component
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

@Component("kafkaRuntime")
class KafkaRuntimeHealthMonitor(
    private val kafkaHealthProperties: KafkaHealthProperties,
    private val clock: Clock,
    private val kafkaHealthMetrics: KafkaHealthMetrics,
) : HealthIndicator {
    private val trackedListeners = ConcurrentHashMap.newKeySet<String>()
    private val listenerStates = ConcurrentHashMap<String, ListenerRuntimeState>()

    fun registerListener(listenerId: String) {
        trackedListeners.add(listenerId)
        listenerStates.computeIfAbsent(listenerId) { ListenerRuntimeState(now()) }
        kafkaHealthMetrics.registerRuntimeListener(listenerId)
    }

    fun onRecordProcessed(listenerId: String) {
        if (!trackedListeners.contains(listenerId)) {
            return
        }
        listenerStates.computeIfAbsent(listenerId) { ListenerRuntimeState(now()) }.markHealthy(now())
        kafkaHealthMetrics.markRuntimeHealthy(listenerId)
    }

    @EventListener
    fun onConsumerStarted(event: ConsumerStartedEvent) {
        listenerIdOf(event)?.takeIf(trackedListeners::contains)?.let { listenerId ->
            listenerStates.computeIfAbsent(listenerId) { ListenerRuntimeState(now()) }.markHealthy(now())
            kafkaHealthMetrics.markRuntimeHealthy(listenerId)
        }
    }

    @EventListener
    fun onListenerContainerIdle(event: ListenerContainerIdleEvent) {
        event.listenerId.takeIf(trackedListeners::contains)?.let { listenerId ->
            listenerStates.computeIfAbsent(listenerId) { ListenerRuntimeState(now()) }.markHealthy(now())
            kafkaHealthMetrics.markRuntimeHealthy(listenerId)
        }
    }

    @EventListener
    fun onNonResponsiveConsumer(event: NonResponsiveConsumerEvent) {
        event.listenerId.takeIf(trackedListeners::contains)?.let { listenerId ->
            listenerStates
                .computeIfAbsent(
                    listenerId,
                ) { ListenerRuntimeState(now()) }
                .markProblem("NON_RESPONSIVE", now())
            kafkaHealthMetrics.markRuntimeProblem(listenerId, "NON_RESPONSIVE")
        }
    }

    @EventListener
    fun onConsumerFailedToStart(event: ConsumerFailedToStartEvent) {
        listenerIdOf(event)?.takeIf(trackedListeners::contains)?.let { listenerId ->
            listenerStates
                .computeIfAbsent(
                    listenerId,
                ) { ListenerRuntimeState(now()) }
                .markProblem("FAILED_TO_START", now())
            kafkaHealthMetrics.markRuntimeProblem(listenerId, "FAILED_TO_START")
        }
    }

    @EventListener
    fun onConsumerStopped(event: ConsumerStoppedEvent) {
        if (event.reason == ConsumerStoppedEvent.Reason.NORMAL) {
            return
        }

        listenerIdOf(event)?.takeIf(trackedListeners::contains)?.let { listenerId ->
            listenerStates
                .computeIfAbsent(listenerId) { ListenerRuntimeState(now()) }
                .markProblem("STOPPED_${event.reason.name}", now())
            kafkaHealthMetrics.markRuntimeProblem(listenerId, "STOPPED_${event.reason.name}")
        }
    }

    override fun health(): Health {
        val now = now()
        val unhealthyListeners =
            trackedListeners
                .mapNotNull { listenerId ->
                    listenerStates[listenerId]
                        ?.takeIf { it.isUnhealthy(now, kafkaHealthProperties.runtimeGracePeriod.toMillis()) }
                        ?.let { listenerId to it.snapshot(now) }
                }.toMap()

        val builder = if (unhealthyListeners.isEmpty()) Health.up() else Health.down()

        return builder
            .withDetail("trackedListeners", trackedListeners.size)
            .withDetail("runtimeGracePeriodMs", kafkaHealthProperties.runtimeGracePeriod.toMillis())
            .withDetail("unhealthyListeners", unhealthyListeners)
            .build()
    }

    private fun listenerIdOf(event: KafkaEvent): String? {
        return runCatching {
            event.getContainer(MessageListenerContainer::class.java).listenerId
        }.getOrNull()
    }

    private fun now(): Long = clock.millis()
}

private class ListenerRuntimeState(
    initialHealthyAt: Long,
) {
    private val lastHealthyAt = AtomicLong(initialHealthyAt)
    private val problemSince = AtomicLong(0L)
    private val problem = AtomicReference<String?>(null)

    fun markHealthy(now: Long) {
        lastHealthyAt.set(now)
        problemSince.set(0L)
        problem.set(null)
    }

    fun markProblem(
        reason: String,
        now: Long,
    ) {
        problem.compareAndSet(null, reason)
        problemSince.compareAndSet(0L, now)
    }

    fun isUnhealthy(
        now: Long,
        gracePeriodMs: Long,
    ): Boolean =
        problemSince
            .get()
            .takeIf { it > 0L }
            ?.let { now - it >= gracePeriodMs }
            ?: false

    fun snapshot(now: Long): Map<String, Any?> =
        mapOf(
            "problem" to problem.get(),
            "problemDurationMs" to (now - problemSince.get()),
            "lastHealthyAtMs" to lastHealthyAt.get(),
        )
}
