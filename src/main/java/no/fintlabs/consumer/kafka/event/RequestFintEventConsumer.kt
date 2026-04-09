package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaConsumerErrorHandling
import no.fintlabs.consumer.resource.event.EventStatusCache
import no.fintlabs.kafka.KafkaConsumerNames.EVENT
import no.fintlabs.kafka.KafkaConsumerNames.RESOURCE
import no.fintlabs.kafka.config.ConfigurableConsumer
import no.fintlabs.kafka.config.KafkaProperties
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

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
