package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EntityTopicService
import no.novari.kafka.topic.configuration.EntityCleanupFrequency
import no.novari.kafka.topic.configuration.EntityTopicConfiguration
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import no.novari.metamodel.MetamodelService
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

@Service
class RelationUpdateProducer(
    private val entityTopicService: EntityTopicService,
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val consumerConfiguration: ConsumerConfiguration,
    private val metamodelService: MetamodelService,
) {
    private val entityProducer = parameterizedTemplateFactory.createTemplate(RelationUpdate::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun ensureTopics() {
        if (!consumerConfiguration.kafka.relationTopicCreation) return
        metamodelService.getComponents().forEach { component ->
            entityTopicService.createOrModifyTopic(
                createTopicNameParameters(component.domainName, component.packageName),
                EntityTopicConfiguration
                    .stepBuilder()
                    .partitions(consumerConfiguration.kafka.relationPartitions)
                    .lastValueRetentionTime(consumerConfiguration.kafka.relationRetentionTime)
                    .nullValueRetentionTime(consumerConfiguration.kafka.relationRetentionTime)
                    .cleanupFrequency(EntityCleanupFrequency.FREQUENT) // Triggers compaction every 3.rd hour
                    .build(),
            )
        }
    }

    fun publishRelationUpdate(
        relationUpdate: RelationUpdate,
        resourceName: String,
        resourceId: String,
    ): CompletableFuture<SendResult<String, RelationUpdate>> =
        with(relationUpdate.targetEntity) {
            entityProducer.send(
                ParameterizedProducerRecord
                    .builder<RelationUpdate>()
                    .key(relationUpdate.toKey(resourceName, resourceId))
                    .topicNameParameters(createTopicNameParameters(domainName, packageName))
                    .value(relationUpdate)
                    .build(),
            )
        }

    /**
     * Builds a unique Kafka message key for this relation update.
     *
     * The key uniquely identifies a single relation slot: the binding between a specific source
     * resource instance and a specific target entity type via a specific relation. This ensures:
     *
     * - **Compaction correctness**: Each relation slot gets its own key, so compaction retains
     *   the latest state per slot without overwriting unrelated relation updates from the same
     *   source resource.
     * - **Partition ordering**: ADD and DELETE for the same relation slot always share the same
     *   key and are therefore routed to the same partition, guaranteeing correct ordering.
     *
     * Format: `{sourceResourceName}/{identifier}#{targetResource}#{relationName}`
     * Example: `elev/abc123#elevforhold#elev`
     *
     * Note: [targetEntity.domainName] and [targetEntity.packageName] are intentionally omitted
     * from the key since the topic itself already encodes that scope.
     */
    internal fun RelationUpdate.toKey(
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
