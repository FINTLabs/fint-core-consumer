package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.CompletableFuture

@Component
class RelationUpdateProducer(
    eventTopicService: EventTopicService,
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val consumerConfiguration: ConsumerConfiguration,
) {
    companion object {
        /**
         * Retention time (7 days) matches the Core 2 maximum to ensure
         * relation updates do not expire before their associated resources.
         */
        val RETENTION_TIME: Duration = Duration.ofDays(7)
        const val PARTITIONS = 1
    }

    private val eventTopic = createEventTopic()
    private val entityProducer = parameterizedTemplateFactory.createTemplate(RelationUpdate::class.java)

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

    fun publishRelationUpdate(relationUpdate: RelationUpdate): CompletableFuture<SendResult<String, RelationUpdate>> =
        entityProducer.send(
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
                    .orgId(consumerConfiguration.orgId.toTopicFormat())
                    .domainContextApplicationDefault()
                    .build(),
            )
            .eventName("relation-update")
            .build()

    private fun String.toTopicFormat() = replace(".", "-")
}
