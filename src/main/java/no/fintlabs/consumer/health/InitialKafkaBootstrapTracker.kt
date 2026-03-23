package no.fintlabs.consumer.health

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.boot.availability.AvailabilityChangeEvent
import org.springframework.boot.availability.ReadinessState
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max

@Service
class InitialKafkaBootstrapTracker(
    private val endOffsetProvider: EndOffsetProvider,
    private val applicationContext: ApplicationContext,
    private val kafkaHealthMetrics: KafkaHealthMetrics,
) {
    private val readinessPublished = AtomicReference<Boolean?>(null)
    private val bootstrapCompleted = AtomicBoolean(false)
    private val blockingListeners = ConcurrentHashMap<String, ListenerBootstrapState>()

    init {
        publishReadiness(false)
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
        val endOffsets =
            try {
                endOffsetProvider.latestOffsets(assignments)
            } catch (exception: RuntimeException) {
                logger.error(
                    "Failed to fetch end offsets for listener={} assignments={}",
                    listenerId,
                    assignments,
                    exception,
                )
                kafkaHealthMetrics.recordBootstrapEndOffsetLookupFailure(listenerId)
                publishReadiness(false)
                return
            }

        listenerState.assignmentSeen.set(true)
        assignments.forEach { topicPartition ->
            listenerState.partitions[topicPartition] = PartitionBootstrapState(endOffsets[topicPartition] ?: 0L)
        }

        maybeCompleteListener(listenerId, listenerState)
        maybeCompleteBootstrap()
        kafkaHealthMetrics.updateBootstrapPendingPartitions(listenerId, pendingPartitions(listenerState))
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
            state.withOffset(record.offset())
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
    val endOffset: Long,
    val processedOffset: Long? = null,
) {
    val caughtUp: Boolean
        get() = endOffset == 0L || ((processedOffset ?: -1L) + 1) >= endOffset

    fun withOffset(offset: Long): PartitionBootstrapState {
        return copy(processedOffset = processedOffset?.let { max(it, offset) } ?: offset)
    }
}
