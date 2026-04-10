package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.RelationEventService
import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.kafka.stringValue
import no.fintlabs.kafka.KafkaConsumerNames.AUTO_RELATION_RESOURCE
import no.fintlabs.kafka.config.ConfigurableConsumer
import no.fintlabs.kafka.config.KafkaProperties
import no.fintlabs.kafka.extractIdentifier
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener

@Configuration
class AutoRelationResourceConsumer(
    private val relationEventService: RelationEventService,
    kafkaProperties: KafkaProperties,
) : ConfigurableConsumer(kafkaProperties, AUTO_RELATION_RESOURCE) {
    @KafkaListener(
        topics = ["#{resourceTopicPattern}"],
        containerFactory = "autoRelationResourceFactory",
        groupId = "\${novari.kafka.application-id}-auto-relation-resource",
    )
    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any?>) {
        consumerRecord
            .value()
            ?.let { resource ->
                relationEventService.addRelations(
                    consumerRecord.getResourceName(),
                    consumerRecord.extractIdentifier(),
                    resource,
                )
            }
    }

    private fun ConsumerRecord<String, Any?>.getResourceName(): String =
        headers().stringValue(RESOURCE_NAME) ?: throw IllegalArgumentException("Resource name header not found")
}
