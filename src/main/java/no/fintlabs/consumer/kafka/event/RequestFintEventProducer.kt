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

@Service
class RequestFintEventProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    eventTopicService: EventTopicService,
    private val consumerConfig: ConsumerConfiguration,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val producer = parameterizedTemplateFactory.createTemplate(RequestFintEvent::class.java)

    private val topicNameParameters =
        EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId(consumerConfig.orgId.asTopicSegment)
                    .domainContextApplicationDefault()
                    .build(),
            ).eventName("${consumerConfig.domain}-${consumerConfig.packageName}-request")
            .build()

    init {
        ensureTopicExists(eventTopicService)
    }

    fun produce(requestFintEvent: RequestFintEvent) {
        logger.info("Publishing RequestFintEvent: {}", requestFintEvent.corrId)
        producer.send(
            ParameterizedProducerRecord
                .builder<RequestFintEvent>()
                .key(requestFintEvent.corrId)
                .topicNameParameters(topicNameParameters)
                .value(requestFintEvent)
                .build(),
        )
    }

    private fun ensureTopicExists(eventTopicService: EventTopicService) {
        eventTopicService.createOrModifyTopic(
            topicNameParameters,
            EventTopicConfiguration
                .stepBuilder()
                .partitions(consumerConfig.kafka.requestPartitions)
                .retentionTime(consumerConfig.kafka.requestRetentionTime)
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }
}
