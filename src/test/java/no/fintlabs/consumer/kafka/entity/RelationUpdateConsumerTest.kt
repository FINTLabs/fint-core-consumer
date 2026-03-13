package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.kafka.RelationUpdateConsumer
import no.fintlabs.autorelation.model.RelationUpdate
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
    private lateinit var consumerRecord: ConsumerRecord<String?, RelationUpdate>
    private lateinit var relationUpdate: RelationUpdate
    private lateinit var kafkaThroughputMetrics: KafkaThroughputMetrics

    @BeforeEach
    fun setUp() {
        autoRelationService = mockk(relaxed = true)
        consumerConfig = mockk()
        relationUpdate = mockk(relaxed = true)
        consumerRecord =
            mockk {
                every { value() } returns relationUpdate
            }

        kafkaThroughputMetrics = mockk(relaxed = true)
        relationUpdateConsumer = RelationUpdateConsumer(autoRelationService, consumerConfig, kafkaThroughputMetrics)
    }

    @Test
    fun `process if consumerConfiguration matches`() {
        val domain = "testdomain"
        val pkg = "pkgtest"

        every { relationUpdate.targetEntity } returns createEntityDescriptor(domain, pkg, "resource")

        every { consumerConfig.matchesComponent(domain, pkg) } returns true

        relationUpdateConsumer.consumeRecord(consumerRecord)

        verify(exactly = 1) { autoRelationService.applyOrBufferUpdate(any()) }
    }
}
