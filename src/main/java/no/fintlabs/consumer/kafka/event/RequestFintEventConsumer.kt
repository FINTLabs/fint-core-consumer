package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.resource.event.EventStatusCache
import no.fintlabs.kafka.KafkaConsumerNames.EVENT
import no.fintlabs.kafka.config.ConfigurableConsumer
import no.fintlabs.kafka.config.KafkaProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener

@Configuration
class RequestFintEventConsumer(
    private val eventStatusCache: EventStatusCache,
    kafkaProperties: KafkaProperties,
) : ConfigurableConsumer(kafkaProperties, EVENT) {
    @KafkaListener(
        topicPattern = "#{eventRequestTopicPattern}",
        containerFactory = "eventFactory",
    )
    private fun consumeRecord(consumerRecord: ConsumerRecord<String?, RequestFintEvent>) =
        eventStatusCache.trackRequest(consumerRecord.value())
}
