package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.KafkaTopicName
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service

@Component
class RequestFintEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, RequestFintEvent>,
    private val eventResponseTopicPattern: String,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun produce(requestFintEvent: RequestFintEvent) {
        logger.info("Producing RequestFintEvent: {}", requestFintEvent.corrId)
        kafkaTemplate.send(eventResponseTopicPattern, requestFintEvent.corrId, requestFintEvent)
    }
}
