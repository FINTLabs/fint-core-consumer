package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class RequestFintEventProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val eventTopicService: EventTopicService,
    private val config: ConsumerConfiguration,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val producer = parameterizedTemplateFactory.createTemplate(RequestFintEvent::class.java)
    private val ensuredTopics = mutableSetOf<String>()

    companion object {
        val RETENTION_TIME: Duration = Duration.ofDays(7)
        const val PARTITIONS = 1
    }

    fun publish(
        resourceName: String,
        requestFintEvent: RequestFintEvent,
    ) {
        val topic = ensureTopic(resourceName)
        producer.send(
            ParameterizedProducerRecord
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
                eventTopicService.createOrModifyTopic(
                    topic,
                    EventTopicConfiguration
                        .stepBuilder()
                        .partitions(PARTITIONS)
                        .retentionTime(RETENTION_TIME)
                        .cleanupFrequency(EventCleanupFrequency.NORMAL)
                        .build(),
                )
            }
        }

    private fun eventTopicFor(resourceName: String): EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId(config.orgId.toTopicFormat())
                    .domainContextApplicationDefault()
                    .build(),
            ).eventName(eventNameFor(resourceName))
            .build()

    private fun eventNameFor(resourceName: String) =
        with(config) {
            "$domain-$packageName-$resourceName-request"
        }

    private fun String.toTopicFormat() = replace(".", "-")
}
