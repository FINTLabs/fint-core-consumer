package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.kafka.RelationUpdateConsumer
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.createEntityDescriptor
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.fintlabs.kafka.config.KafkaProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RelationUpdateConsumerTest {
    private lateinit var autoRelationService: AutoRelationService
    private lateinit var relationUpdateConsumer: RelationUpdateConsumer
    private lateinit var kafkaThroughputMetrics: KafkaThroughputMetrics

    @BeforeEach
    fun setUp() {
        autoRelationService = mockk(relaxed = true)
        kafkaThroughputMetrics = mockk(relaxed = true)
        relationUpdateConsumer = RelationUpdateConsumer(autoRelationService, kafkaThroughputMetrics, KafkaProperties())
    }

    @Test
    fun `non-null relation update is processed`() {
        val relationUpdate = mockk<RelationUpdate>(relaxed = true) {
            every { targetEntity } returns createEntityDescriptor("utdanning", "vurdering", "elev")
        }
        val consumerRecord =
            mockk<ConsumerRecord<String?, RelationUpdate>> {
                every { value() } returns relationUpdate
            }

        relationUpdateConsumer.consumeRecord(consumerRecord)

        verify(exactly = 1) { autoRelationService.applyOrBufferUpdate(relationUpdate) }
    }

    @Test
    fun `null relation update is ignored`() {
        val consumerRecord =
            mockk<ConsumerRecord<String?, RelationUpdate>> {
                every { value() } returns null
            }

        relationUpdateConsumer.consumeRecord(consumerRecord)

        verify(exactly = 0) { autoRelationService.applyOrBufferUpdate(any()) }
    }
}
