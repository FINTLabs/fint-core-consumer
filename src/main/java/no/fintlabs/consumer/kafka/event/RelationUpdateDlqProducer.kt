package no.fintlabs.consumer.kafka.event

import no.fintlabs.autorelation.model.RelationUpdate
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
class RelationUpdateDlqProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    eventTopicService: EventTopicService,
) {
    private val eventProducer = parameterizedTemplateFactory.createTemplate(RelationUpdate::class.java)
    private val eventTopic = createEventTopic()

    init {
        eventTopicService.createOrModifyTopic(
            createEventTopic(),
            EventTopicConfiguration
                .stepBuilder()
                .partitions(1)
                .retentionTime(Duration.ofDays(7))
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }

    fun publish(relationUpdate: RelationUpdate) =
        eventProducer.send(
            ParameterizedProducerRecord
                .builder<RelationUpdate>()
                .topicNameParameters(eventTopic)
                .value(relationUpdate)
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
            ).eventName("relation-update-dlq")
            .build()
}
