package no.fintlabs.consumer.exception.kafka

import no.fintlabs.status.models.error.ConsumerError
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.*

@Component
class ConsumerErrorPublisher(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    eventTopicService: EventTopicService,
) {
    private val eventProducer = parameterizedTemplateFactory.createTemplate(ConsumerError::class.java)
    private val logger = LoggerFactory.getLogger(javaClass)
    private val eventName = createEventName()

    init {
        eventTopicService.createOrModifyTopic(
            eventName,
            EventTopicConfiguration
                .stepBuilder()
                .partitions(1)
                .retentionTime(Duration.ofDays(7))
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }

    fun publish(consumerError: ConsumerError) {
        logger.info("Publishing consumer-error to Kafka")
        eventProducer.send(
            ParameterizedProducerRecord
                .builder<ConsumerError>()
                .key(
                    UUID.randomUUID().toString(),
                ) // TODO: Add some consumer instance id? So we know if its related to the correct pod
                .topicNameParameters(eventName)
                .value(consumerError)
                .build(),
        )
    }

    private fun createEventName(): EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId("fintlabs-no")
                    .domainContextApplicationDefault()
                    .build(),
            ).eventName("consumer-error")
            .build()
}
