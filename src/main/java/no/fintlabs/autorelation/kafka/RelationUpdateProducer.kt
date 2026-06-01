package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.model.RelationState
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture

@Component
class RelationUpdateProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val consumerConfiguration: ConsumerConfiguration,
    private val kafkaThroughputMetrics: KafkaThroughputMetrics,
) {
    private val entityProducer = parameterizedTemplateFactory.createTemplate(RelationState::class.java)

    fun publish(
        relationState: RelationState,
        resourceName: String,
        resourceId: String,
    ): CompletableFuture<SendResult<String, RelationState>> {
        val targetEntity = relationState.targetEntity

        val result =
            entityProducer.send(
                ParameterizedProducerRecord
                    .builder<RelationState>()
                    .key(relationState.toKey(resourceName, resourceId))
                    .topicNameParameters(createTopicNameParameters(targetEntity.domainName, targetEntity.packageName))
                    .value(relationState)
                    .build(),
            )

        result.whenComplete { _, throwable ->
            kafkaThroughputMetrics.recordRelationStateProduced(
                targetEntity.resourceName,
                if (throwable == null) "published" else "failed",
            )
        }
        return result
    }

    /**
     * Builds the Kafka message key for this relation state.
     *
     * The key uniquely identifies a single relation slot: the binding between a specific source
     * resource instance and a specific target entity type via a specific relation. This is what
     * makes the compacted topic carry *state*: each new state for a slot fully supersedes the
     * previous one under the same key, and compaction retains the latest per slot.
     *
     * Format: `{sourceResourceName}/{identifier}#{targetResource}#{relationName}`
     * Example: `elev/abc123#elevforhold#elev`
     *
     * [targetEntity.domainName] and [targetEntity.packageName] are intentionally omitted since the
     * topic itself already encodes that scope.
     */
    internal fun RelationState.toKey(
        resourceName: String,
        resourceId: String,
    ): String = "$resourceName/$resourceId#${targetEntity.resourceName}#${binding.relationName}"

    private fun createTopicNameParameters(
        domainName: String,
        packageName: String,
    ) = EntityTopicNameParameters
        .builder()
        .topicNamePrefixParameters(
            TopicNamePrefixParameters
                .stepBuilder()
                .orgId(consumerConfiguration.orgId.asTopicSegment)
                .domainContextApplicationDefault()
                .build(),
        ).resourceName("$domainName-$packageName-relation-update")
        .build()
}
