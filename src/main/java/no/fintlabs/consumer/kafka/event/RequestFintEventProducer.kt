package no.fintlabs.consumer.kafka.event

import mu.KotlinLogging
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicService
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class RequestFintEventProducer(
    eventProducerFactory: EventProducerFactory,
    private val eventTopicService: EventTopicService,
    private val config: ConsumerConfiguration,
) {
    private val logger = KotlinLogging.logger {}
    private val producer = eventProducerFactory.createProducer(RequestFintEvent::class.java)
    private val ensuredTopics = mutableSetOf<String>()
    private val retention = Duration.ofDays(7)

    fun publish(
        resourceName: String,
        requestFintEvent: RequestFintEvent,
    ) {
        val topic = ensureTopic(resourceName)
        producer.send(
            EventProducerRecord
                .builder<RequestFintEvent>()
                .key(requestFintEvent.corrId)
                .topicNameParameters(topic)
                .value(requestFintEvent)
                .build(),
        )
    }

    private fun ensureTopic(resourceName: String): EventTopicNameParameters =
        eventTopicFor(resourceName).also { topic ->
            if (ensuredTopics.add(topic.eventName)) {
                logger.debug("Ensuring event topic: {}", topic.eventName)
                eventTopicService.ensureTopic(topic, retention.toMillis())
            }
        }

    private fun eventTopicFor(resourceName: String): EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .eventName("${config.domain}-${config.packageName}-$resourceName")
            .build()
}
