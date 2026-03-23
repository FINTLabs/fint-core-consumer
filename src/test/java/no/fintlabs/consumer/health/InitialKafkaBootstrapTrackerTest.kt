package no.fintlabs.consumer.health

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationContext
import java.time.Duration
import java.time.Instant

class InitialKafkaBootstrapTrackerTest {
    private val endOffsetProvider: EndOffsetProvider = mockk()
    private val applicationContext: ApplicationContext = mockk(relaxed = true)
    private val meterRegistry = SimpleMeterRegistry()
    private val clock = MutableClock(Instant.parse("2026-03-20T10:00:00Z"))
    private val kafkaHealthMetrics = KafkaHealthMetrics(meterRegistry, clock, KafkaHealthProperties())

    private lateinit var tracker: InitialKafkaBootstrapTracker

    @BeforeEach
    fun setUp() {
        tracker = InitialKafkaBootstrapTracker(endOffsetProvider, applicationContext, kafkaHealthMetrics)
        tracker.registerBlockingListener(KafkaListenerIds.ENTITY)
    }

    @Test
    fun `should stay unready until all assigned partitions catch up`() {
        val partition0 = TopicPartition("topic", 0)
        val partition1 = TopicPartition("topic", 1)

        every { endOffsetProvider.latestOffsets(setOf(partition0, partition1)) } returns
            mapOf(partition0 to 2L, partition1 to 1L)

        tracker.onPartitionsAssigned(KafkaListenerIds.ENTITY, setOf(partition0, partition1))

        assertFalse(tracker.snapshot().ready)

        tracker.onRecordProcessed(KafkaListenerIds.ENTITY, ConsumerRecord("topic", 0, 0L, "key", "value"))
        assertFalse(tracker.snapshot().ready)

        tracker.onRecordProcessed(KafkaListenerIds.ENTITY, ConsumerRecord("topic", 1, 0L, "key", "value"))
        assertFalse(tracker.snapshot().ready)

        tracker.onRecordProcessed(KafkaListenerIds.ENTITY, ConsumerRecord("topic", 0, 1L, "key", "value"))
        assertTrue(tracker.snapshot().ready)
    }

    @Test
    fun `should become ready immediately when assigned partitions are empty at startup offset`() {
        val partition0 = TopicPartition("topic", 0)

        every { endOffsetProvider.latestOffsets(setOf(partition0)) } returns
            mapOf(partition0 to 0L)

        tracker.onPartitionsAssigned(KafkaListenerIds.ENTITY, setOf(partition0))

        assertTrue(tracker.snapshot().ready)
    }

    @Test
    fun `should ignore new assignments after initial bootstrap has completed`() {
        val partition0 = TopicPartition("topic", 0)
        val partition1 = TopicPartition("topic", 1)

        every { endOffsetProvider.latestOffsets(setOf(partition0)) } returns mapOf(partition0 to 1L)
        every { endOffsetProvider.latestOffsets(setOf(partition1)) } returns mapOf(partition1 to 2L)

        tracker.onPartitionsAssigned(KafkaListenerIds.ENTITY, setOf(partition0))
        tracker.onRecordProcessed(KafkaListenerIds.ENTITY, ConsumerRecord("topic", 0, 0L, "key", "value"))

        assertTrue(tracker.snapshot().ready)

        tracker.onPartitionsAssigned(KafkaListenerIds.ENTITY, setOf(partition1))

        assertTrue(tracker.snapshot().ready)
    }

    @Test
    fun `should wait for both entity and relation update listeners`() {
        val entityPartition = TopicPartition("entity-topic", 0)
        val relationPartition = TopicPartition("relation-topic", 0)

        tracker.registerBlockingListener(KafkaListenerIds.RELATION_UPDATE)

        every { endOffsetProvider.latestOffsets(setOf(entityPartition)) } returns mapOf(entityPartition to 1L)
        every { endOffsetProvider.latestOffsets(setOf(relationPartition)) } returns mapOf(relationPartition to 1L)

        tracker.onPartitionsAssigned(KafkaListenerIds.ENTITY, setOf(entityPartition))
        tracker.onPartitionsAssigned(KafkaListenerIds.RELATION_UPDATE, setOf(relationPartition))
        tracker.onRecordProcessed(KafkaListenerIds.ENTITY, ConsumerRecord("entity-topic", 0, 0L, "key", "value"))

        assertFalse(tracker.snapshot().ready)

        tracker.onRecordProcessed(
            KafkaListenerIds.RELATION_UPDATE,
            ConsumerRecord("relation-topic", 0, 0L, "key", "value"),
        )

        assertTrue(tracker.snapshot().ready)
    }

    @Test
    fun `should publish bootstrap metrics for completion and pending partitions`() {
        val partition0 = TopicPartition("topic", 0)
        val partition1 = TopicPartition("topic", 1)

        every { endOffsetProvider.latestOffsets(setOf(partition0, partition1)) } returns
            mapOf(partition0 to 2L, partition1 to 1L)

        tracker.onPartitionsAssigned(KafkaListenerIds.ENTITY, setOf(partition0, partition1))

        assertEquals(
            2.0,
            meterRegistry
                .get("fint.consumer.kafka.bootstrap.partitions.pending")
                .tag("listener", KafkaListenerIds.ENTITY)
                .gauge()
                .value(),
        )
        assertEquals(
            1.0,
            meterRegistry
                .get("fint.consumer.kafka.bootstrap.state")
                .tag("listener", KafkaListenerIds.ENTITY)
                .gauge()
                .value(),
        )

        clock.advance(Duration.ofSeconds(2))
        tracker.onRecordProcessed(KafkaListenerIds.ENTITY, ConsumerRecord("topic", 0, 0L, "key", "value"))
        tracker.onRecordProcessed(KafkaListenerIds.ENTITY, ConsumerRecord("topic", 1, 0L, "key", "value"))
        tracker.onRecordProcessed(KafkaListenerIds.ENTITY, ConsumerRecord("topic", 0, 1L, "key", "value"))

        assertEquals(
            0.0,
            meterRegistry
                .get("fint.consumer.kafka.bootstrap.partitions.pending")
                .tag("listener", KafkaListenerIds.ENTITY)
                .gauge()
                .value(),
        )
        assertEquals(
            0.0,
            meterRegistry
                .get("fint.consumer.kafka.bootstrap.state")
                .tag("listener", KafkaListenerIds.ENTITY)
                .gauge()
                .value(),
        )
        assertEquals(
            1.0,
            meterRegistry
                .get("fint.consumer.kafka.bootstrap.completed")
                .tag("listener", KafkaListenerIds.ENTITY)
                .counter()
                .count(),
        )
        assertEquals(
            1.0,
            meterRegistry
                .get("fint.consumer.kafka.bootstrap.completed")
                .tag("listener", "all")
                .counter()
                .count(),
        )
    }

    @Test
    fun `should count end offset lookup failures`() {
        val partition0 = TopicPartition("topic", 0)

        every { endOffsetProvider.latestOffsets(setOf(partition0)) } throws RuntimeException("boom")

        tracker.onPartitionsAssigned(KafkaListenerIds.ENTITY, setOf(partition0))

        assertEquals(
            1.0,
            meterRegistry
                .get("fint.consumer.kafka.bootstrap.end_offset.lookup.failures")
                .tag("listener", KafkaListenerIds.ENTITY)
                .counter()
                .count(),
        )
    }
}
