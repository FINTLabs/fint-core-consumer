package no.fintlabs.consumer.kafka.sync

import no.fintlabs.consumer.kafka.sync.model.SyncStatus
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
class SyncStatusProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    eventTopicService: EventTopicService,
) {
    private val eventProducer = parameterizedTemplateFactory.createTemplate(SyncStatus::class.java)
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

    fun publish(syncStatus: SyncStatus) =
        eventProducer.send(
            ParameterizedProducerRecord
                .builder<SyncStatus>()
                .topicNameParameters(eventTopic)
                .value(syncStatus)
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
            ).eventName("sync-status")
            .build()
}
