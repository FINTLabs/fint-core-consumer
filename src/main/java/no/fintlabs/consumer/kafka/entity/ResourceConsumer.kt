package no.fintlabs.consumer.kafka.entity

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
        topics = ["#{resourceTopicPattern}"],
        containerFactory = "resourceFactory",
    )
    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any?>) =
        resourceProcessingService.processResourceMessage(consumerRecord.toResourceMessage(resourceConverter))
}
