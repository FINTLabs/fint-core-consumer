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
import java.util.concurrent.CompletableFuture
import org.springframework.kafka.support.SendResult

@Component
class SyncStatusProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    eventTopicService: EventTopicService,
) {
    private val eventProducer = parameterizedTemplateFactory.createTemplate(SyncStatus::class.java)
    private val eventTopic = createEventTopic()

    companion object {
        val RETENTION_TIME: Duration = Duration.ofDays(7)
        const val PARTITIONS = 1
    }

    init {
        eventTopicService.createOrModifyTopic(
            eventTopic,
            EventTopicConfiguration
                .stepBuilder()
                .partitions(PARTITIONS)
                .retentionTime(RETENTION_TIME)
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }

    fun publish(syncStatus: SyncStatus): CompletableFuture<SendResult<String?, SyncStatus?>?>? {
        return eventProducer.send(
            ParameterizedProducerRecord
                .builder<SyncStatus>()
                .topicNameParameters(eventTopic)
                .value(syncStatus)
                .build(),
        )
    }

    private fun createEventTopic() =
        EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId("fintlabs-no")
                    .domainContextApplicationDefault()
                    .build(),
            )
            .eventName("sync-status")
            .build()
}
