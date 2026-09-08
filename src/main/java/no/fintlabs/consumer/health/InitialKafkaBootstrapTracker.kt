package no.fintlabs.consumer.health

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.boot.availability.AvailabilityChangeEvent
import org.springframework.boot.availability.ReadinessState
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max

@Service
class InitialKafkaBootstrapTracker(
    private val endOffsetProvider: EndOffsetProvider,
    private val applicationContext: ApplicationContext,
    private val kafkaHealthMetrics: KafkaHealthMetrics,
    private val kafkaHealthProperties: KafkaHealthProperties,
) {
    private val readinessPublished = AtomicReference<Boolean?>(null)
    private val bootstrapCompleted = AtomicBoolean(false)
    private val blockingListeners = ConcurrentHashMap<String, ListenerBootstrapState>()
    private val executorRef = AtomicReference<ScheduledExecutorService?>(null)

    init {
        publishReadiness(false)
    }

    @PostConstruct
    fun startEndOffsetRefresh() {
        val executor =
            Executors.newSingleThreadScheduledExecutor { runnable ->
                Thread(runnable, "kafka-bootstrap-end-offsets").apply { isDaemon = true }
            }
        executorRef.set(executor)
        val intervalMs = kafkaHealthProperties.bootstrapEndOffsetRefreshInterval.toMillis().coerceAtLeast(1L)
        executor.scheduleWithFixedDelay(::tickRefresh, intervalMs, intervalMs, TimeUnit.MILLISECONDS)
    }

    @PreDestroy
    fun stopEndOffsetRefresh() {
        val executor = executorRef.getAndSet(null) ?: return
        executor.shutdown()
        try {
            val timeoutMs = kafkaHealthProperties.bootstrapEndOffsetExecutorShutdownTimeout.toMillis()
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow()
            }
        } catch (_: InterruptedException) {
            executor.shutdownNow()
            Thread.currentThread().interrupt()
        }
    }

    fun registerBlockingListener(listenerId: String) {
        blockingListeners.computeIfAbsent(listenerId) { ListenerBootstrapState() }
        kafkaHealthMetrics.registerBootstrapListener(listenerId)
    }

    fun onPartitionsAssigned(
        listenerId: String,
        assignments: Set<TopicPartition>,
    ) {
        if (bootstrapCompleted.get() || assignments.isEmpty()) {
            return
        }

        val listenerState = blockingListeners[listenerId] ?: return

        assignments.forEach { topicPartition ->
            listenerState.partitions.computeIfAbsent(topicPartition) {
                PartitionBootstrapState(endOffset = null)
            }
        }
        listenerState.assignmentSeen.set(true)

        kafkaHealthMetrics.updateBootstrapPendingPartitions(listenerId, pendingPartitions(listenerState))

        triggerImmediateRefresh()
    }

    fun onPartitionsRevoked(
        listenerId: String,
        partitions: Collection<TopicPartition>,
    ) {
        if (bootstrapCompleted.get()) {
            return
        }

        val listenerState = blockingListeners[listenerId] ?: return
        partitions.forEach(listenerState.partitions::remove)
        maybeCompleteListener(listenerId, listenerState)
        maybeCompleteBootstrap()
        kafkaHealthMetrics.updateBootstrapPendingPartitions(listenerId, pendingPartitions(listenerState))
    }

    fun onRecordProcessed(
        listenerId: String,
        record: ConsumerRecord<*, *>,
    ) {
        if (bootstrapCompleted.get()) {
            return
        }

        val listenerState = blockingListeners[listenerId] ?: return
        val topicPartition = TopicPartition(record.topic(), record.partition())
        listenerState.partitions.computeIfPresent(topicPartition) { _, state ->
            state.withProcessedOffset(record.offset())
        }
        maybeCompleteListener(listenerId, listenerState)
        maybeCompleteBootstrap()
        kafkaHealthMetrics.updateBootstrapPendingPartitions(listenerId, pendingPartitions(listenerState))
    }

    fun snapshot(): BootstrapReadinessSnapshot {
        val listenerStatuses =
            blockingListeners.toSortedMap().map { (listenerId, listenerState) ->
                ListenerBootstrapStatus(
                    listenerId = listenerId,
                    assignmentSeen = listenerState.assignmentSeen.get(),
                    completed = listenerState.completed.get(),
                    assignedPartitions = listenerState.partitions.size,
                    caughtUpPartitions = listenerState.partitions.values.count(PartitionBootstrapState::caughtUp),
                    partitions =
                        listenerState.partitions
                            .toSortedMap(compareBy<TopicPartition>({ it.topic() }, { it.partition() }))
                            .map { (topicPartition, partitionState) ->
                                BootstrapPartitionStatus(
                                    partition = "${topicPartition.topic()}-${topicPartition.partition()}",
                                    endOffset = partitionState.endOffset,
                                    processedOffset = partitionState.processedOffset,
                                    caughtUp = partitionState.caughtUp,
                                )
                            },
                )
            }

        return BootstrapReadinessSnapshot(
            ready = bootstrapCompleted.get(),
            blockingListeners = listenerStatuses,
        )
    }

    internal fun refreshPendingEndOffsets() {
        if (bootstrapCompleted.get()) {
            return
        }

        val pendingByListener =
            blockingListeners
                .mapValues { (_, state) ->
                    state.partitions
                        .entries
                        .asSequence()
                        .filter { it.value.endOffset == null }
                        .map { it.key }
                        .toSet()
                }.filterValues { it.isNotEmpty() }

        if (pendingByListener.isEmpty()) {
            return
        }

        val allPartitions = pendingByListener.values.flatten().toSet()
        val results =
            try {
                endOffsetProvider.latestOffsets(allPartitions)
            } catch (exception: Exception) {
                logger.warn(
                    "End-offset lookup failed for {} partitions across {} listeners; will retry",
                    allPartitions.size,
                    pendingByListener.size,
                    exception,
                )
                pendingByListener.keys.forEach(kafkaHealthMetrics::recordBootstrapEndOffsetLookupFailure)
                return
            }

        var anyApplied = false
        pendingByListener.forEach { (listenerId, partitions) ->
            val listenerState = blockingListeners[listenerId] ?: return@forEach
            var listenerUpdated = false
            partitions.forEach { topicPartition ->
                val offset = results[topicPartition] ?: return@forEach
                listenerState.partitions.computeIfPresent(topicPartition) { _, existing ->
                    if (existing.endOffset == null) {
                        listenerUpdated = true
                        existing.copy(endOffset = offset)
                    } else {
                        existing
                    }
                }
            }
            if (listenerUpdated) {
                anyApplied = true
                maybeCompleteListener(listenerId, listenerState)
                kafkaHealthMetrics.updateBootstrapPendingPartitions(listenerId, pendingPartitions(listenerState))
            }
        }

        if (anyApplied) {
            maybeCompleteBootstrap()
        }
    }

    private fun tickRefresh() {
        try {
            refreshPendingEndOffsets()
        } catch (exception: Exception) {
            logger.error("Unexpected error during end-offset refresh tick", exception)
        }
    }

    private fun triggerImmediateRefresh() {
        val executor = executorRef.get() ?: return
        try {
            executor.execute(::tickRefresh)
        } catch (_: RejectedExecutionException) {
            // Executor is shutting down — the next scheduled tick (if any) will pick this up.
        }
    }

    private fun maybeCompleteListener(
        listenerId: String,
        listenerState: ListenerBootstrapState,
    ) {
        if (
            !listenerState.completed.get() &&
            listenerState.assignmentSeen.get() &&
            listenerState.partitions.values.all(PartitionBootstrapState::caughtUp)
        ) {
            listenerState.completed.set(true)
            kafkaHealthMetrics.markBootstrapCompleted(listenerId)
            logger.info("Initial Kafka bootstrap completed for listener={}", listenerId)
        }
    }

    private fun maybeCompleteBootstrap() {
        if (
            !bootstrapCompleted.get() &&
            blockingListeners.isNotEmpty() &&
            blockingListeners.values.all { it.completed.get() }
        ) {
            bootstrapCompleted.set(true)
            kafkaHealthMetrics.markBootstrapAllCompleted()
            publishReadiness(true)
            logger.info("Initial Kafka bootstrap completed for all blocking listeners")
        }
    }

    private fun pendingPartitions(listenerState: ListenerBootstrapState): Int {
        return listenerState.partitions.values.count { !it.caughtUp }
    }

    private fun publishReadiness(ready: Boolean) {
        val changed = readinessPublished.getAndSet(ready) != ready
        if (changed) {
            AvailabilityChangeEvent.publish(
                applicationContext,
                if (ready) ReadinessState.ACCEPTING_TRAFFIC else ReadinessState.REFUSING_TRAFFIC,
            )
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(InitialKafkaBootstrapTracker::class.java)
    }
}

private class ListenerBootstrapState {
    val completed = AtomicBoolean(false)
    val assignmentSeen = AtomicBoolean(false)
    val partitions = ConcurrentHashMap<TopicPartition, PartitionBootstrapState>()
}

private data class PartitionBootstrapState(
    val endOffset: Long?,
    val processedOffset: Long? = null,
) {
    val caughtUp: Boolean
        get() {
            val end = endOffset ?: return false
            return end == 0L || ((processedOffset ?: -1L) + 1) >= end
        }

    fun withProcessedOffset(offset: Long): PartitionBootstrapState {
        return copy(processedOffset = processedOffset?.let { max(it, offset) } ?: offset)
    }
}
