package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture

@Component
class RelationUpdateProducer(
    eventTopicService: EventTopicService,
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val consumerConfiguration: ConsumerConfiguration,
    private val kafkaThroughputMetrics: KafkaThroughputMetrics,
) {
    private val eventTopic = createEventTopic()
    private val entityProducer = parameterizedTemplateFactory.createTemplate(RelationUpdate::class.java)

    init {
        eventTopicService.createOrModifyTopic(
            eventTopic,
            EventTopicConfiguration
                .stepBuilder()
                .partitions(consumerConfiguration.kafka.relationPartitions)
                .retentionTime(consumerConfiguration.kafka.relationRetentionTime)
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }

    fun publishRelationUpdate(relationUpdate: RelationUpdate): CompletableFuture<SendResult<String, RelationUpdate>> {
        val targetResource = relationUpdate.targetEntity.resourceName
        val operation = relationUpdate.operation.name
        kafkaThroughputMetrics.recordRelationUpdateProduced(targetResource, operation, "attempted")

        val result =
            entityProducer.send(
                ParameterizedProducerRecord
                    .builder<RelationUpdate>()
                    .topicNameParameters(eventTopic)
                    .value(relationUpdate)
                    .build(),
            )
        result.whenComplete { _, throwable ->
            if (throwable == null) {
                kafkaThroughputMetrics.recordRelationUpdateProduced(targetResource, operation, "published")
            } else {
                kafkaThroughputMetrics.recordRelationUpdateProduced(targetResource, operation, "failed")
            }
        }
        return result
    }

    private fun createEventTopic() =
        EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId(consumerConfiguration.orgId.asTopicSegment)
                    .domainContextApplicationDefault()
                    .build(),
            ).eventName("relation-update")
            .build()
}
