package no.fintlabs.consumer.kafka.event

import no.fintlabs.autorelation.model.RelationRequest
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class RelationRequestProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    eventTopicService: EventTopicService,
) {
    private val eventProducer = parameterizedTemplateFactory.createTemplate(RelationRequest::class.java)
    private val eventTopic = createEventTopic()

    init {
        eventTopicService.createOrModifyTopic(
            createEventTopic(),
            EventTopicConfiguration
                .stepBuilder()
                .partitions(1)
                .retentionTime(Duration.ofHours(3))
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }

    fun publish(relationRequest: RelationRequest) =
        eventProducer.send(
            ParameterizedProducerRecord
                .builder<RelationRequest>()
                .topicNameParameters(eventTopic)
                .value(relationRequest)
                .build(),
        )

    private fun createEventTopic() =
        EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId("fintlabs-no")
                    .domainContextApplicationDefault()
                    .build(),
            ).eventName("relation-request")
            .build()
}
