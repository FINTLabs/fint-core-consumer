package no.fintlabs.consumer.kafka.entity

import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.kafka.stringValue
import no.fintlabs.consumer.resource.ResourceConverter
import no.fintlabs.kafka.KafkaConsumerNames.RESOURCE
import no.fintlabs.kafka.config.ConfigurableConsumer
import no.fintlabs.kafka.config.KafkaProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class ResourceConsumer(
    private val resourceProcessingService: ResourceProcessingService,
    private val resourceConverter: ResourceConverter,
    kafkaProperties: KafkaProperties,
) : ConfigurableConsumer(kafkaProperties, RESOURCE) {
    @KafkaListener(
        topicPattern = "#{resourceTopicPattern}",
        containerFactory = "resourceFactory",
    )
    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any?>) =
        resourceProcessingService.processResourceConsumerRecord(createEntityConsumerRecord(consumerRecord))

    private fun createEntityConsumerRecord(consumerRecord: ConsumerRecord<String, Any?>) =
        consumerRecord.getResourceName().let { resourceName ->
            consumerRecord
                .value()
                ?.let { resourceConverter.convert(resourceName, it) }
                ?.let { ResourceConsumerRecord(resourceName, it, consumerRecord) }
                ?: ResourceConsumerRecord(resourceName, null, consumerRecord)
        }

    private fun ConsumerRecord<String, Any?>.getResourceName(): String =
        headers().stringValue(RESOURCE_NAME) ?: throw IllegalArgumentException("Resource name header not found")
}
