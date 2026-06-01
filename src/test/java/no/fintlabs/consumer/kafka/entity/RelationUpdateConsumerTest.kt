package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.kafka.RelationUpdateConsumer
import no.fintlabs.autorelation.model.RelationState
import no.fintlabs.autorelation.model.createEntityDescriptor
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RelationUpdateConsumerTest {
    private lateinit var autoRelationService: AutoRelationService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var relationUpdateConsumer: RelationUpdateConsumer
    private lateinit var consumerRecord: ConsumerRecord<String?, RelationState>
    private lateinit var relationState: RelationState
    private lateinit var kafkaThroughputMetrics: KafkaThroughputMetrics

    @BeforeEach
    fun setUp() {
        autoRelationService = mockk(relaxed = true)
        consumerConfig = mockk()
        relationState = mockk(relaxed = true)
        consumerRecord = mockk { every { value() } returns relationState }
        kafkaThroughputMetrics = mockk(relaxed = true)
        relationUpdateConsumer = RelationUpdateConsumer(autoRelationService, consumerConfig, kafkaThroughputMetrics)
    }

    @Test
    fun `processes the consumed relation state`() {
        every { relationState.targetEntity } returns createEntityDescriptor("testdomain", "pkgtest", "resource")

        relationUpdateConsumer.consumeRecord(consumerRecord)

        verify(exactly = 1) { autoRelationService.process(any()) }
    }

    @Test
    fun `records consumer metric when processing completes`() {
        every { relationState.targetEntity } returns createEntityDescriptor("d", "p", "resource")

        relationUpdateConsumer.consumeRecord(consumerRecord)

        verify(exactly = 1) { kafkaThroughputMetrics.recordRelationUpdateConsumer("resource", any()) }
    }
}
