package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class RequestFintEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, RequestFintEvent>,
    private val eventRequestTopicPattern: String,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun produce(requestFintEvent: RequestFintEvent) {
        logger.info("Producing RequestFintEvent: {}", requestFintEvent.corrId)
        kafkaTemplate.send(eventRequestTopicPattern, requestFintEvent.corrId, requestFintEvent)
    }
}
