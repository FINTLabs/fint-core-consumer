package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.fintlabs.kafka.KafkaConsumerNames.RELATION_UPDATE
import no.fintlabs.kafka.config.ConfigurableConsumer
import no.fintlabs.kafka.config.KafkaProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener

@Configuration
class RelationUpdateConsumer(
    private val autoRelationService: AutoRelationService,
    private val kafkaThroughputMetrics: KafkaThroughputMetrics,
    kafkaProperties: KafkaProperties,
) : ConfigurableConsumer(kafkaProperties, RELATION_UPDATE) {
    @KafkaListener(
        topics = ["#{relationUpdateTopicPattern}"],
        containerFactory = "relationUpdateFactory",
    )
    fun consumeRecord(consumerRecord: ConsumerRecord<String?, RelationUpdate>) {
        val startedAt = System.nanoTime()
        val relationUpdate = consumerRecord.value()

        if (relationUpdate == null) {
            kafkaThroughputMetrics.recordRelationUpdateConsumer(null, "ignored_null", System.nanoTime() - startedAt)
            return
        }

        try {
            autoRelationService.applyOrBufferUpdate(relationUpdate)
            kafkaThroughputMetrics.recordRelationUpdateConsumer(
                relationUpdate.targetEntity.resourceName,
                "processed",
                System.nanoTime() - startedAt,
            )
        } catch (ex: Exception) {
            kafkaThroughputMetrics.recordRelationUpdateConsumer(
                relationUpdate.targetEntity.resourceName,
                "failed",
                System.nanoTime() - startedAt,
            )
            throw ex
        }
    }
}
