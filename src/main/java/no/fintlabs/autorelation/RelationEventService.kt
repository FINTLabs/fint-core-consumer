package no.fintlabs.autorelation

import mu.KotlinLogging
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.kafka.RelationUpdateProducer
import no.fintlabs.autorelation.model.*
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceConverter
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import org.springframework.stereotype.Service

@Service
class RelationEventService(
    private val resourceConverter: ResourceConverter,
    private val relationRuleRegistry: RelationRuleRegistry,
    private val consumerConfiguration: ConsumerConfiguration,
    private val relationUpdateProducer: RelationUpdateProducer,
    private val metricService: MetricService,
) {
    private val logger = KotlinLogging.logger {}

    fun addRelations(
        resourceName: String,
        resourceId: String,
        resource: Any,
    ) {
        val rules = fetchRules(resourceName).ifEmpty { return }

        runCatching {
            resourceConverter.convert(resourceName, resource)
        }.onSuccess { convertedResource ->
            publishUpdates(resourceName, rules, convertedResource, resourceId, RelationOperation.ADD)
        }.onFailure { error ->
            metricService.incrementRelationFailure(resourceId, resourceName, MetricReason.CONVERSION_FAILED)
            error.log(
                resourceName,
                resourceId,
                MetricReason.CONVERSION_FAILED,
                "Failed to convert resource '$resourceName' with ID '$resourceId'",
            )
        }
    }

    fun removeObsoleteRelations(
        resourceName: String,
        resourceId: String,
        currentResource: FintResource,
        obsoleteLinks: Map<String, List<Link>>,
        rules: List<RelationSyncRule>,
    ) {
        obsoleteLinks.forEach { (relationName, linksToDelete) ->
            val rule = rules.firstOrNull { it.targetRelation == relationName } ?: return@forEach

            runCatching {
                val targetIds = linksToDelete.mapNotNull { it.getIdentifier() }

                if (targetIds.isNotEmpty()) {
                    val update =
                        RelationUpdate(
                            targetEntity = rule.targetType,
                            targetIds = targetIds,
                            binding = rule.toRelationBinding(currentResource, resourceId),
                            operation = RelationOperation.DELETE,
                        )

                    relationUpdateProducer.publishRelationUpdate(update)
                    metricService.incrementRelationSuccess(resourceName)
                }
            }.onFailure { error ->
                val reason = error.getReason()
                metricService.incrementRelationFailure(resourceId, resourceName, reason)
                error.log(
                    resourceName,
                    resourceId,
                    reason,
                    "Failed to publish DELETE update for obsolete links in '$resourceName' ($resourceId). Relation: $relationName",
                )
            }
        }
    }

    fun removeRelations(
        resourceName: String,
        resourceId: String,
        resource: FintResource,
    ) = fetchRules(resourceName)
        .takeIf { it.isNotEmpty() }
        ?.run { publishUpdates(resourceName, this, resource, resourceId, RelationOperation.DELETE) }

    private fun publishUpdates(
        resourceName: String,
        rules: List<RelationSyncRule>,
        resource: FintResource,
        resourceId: String,
        operation: RelationOperation,
    ) {
        rules.forEach { rule ->
            runCatching {
                rule.toRelationUpdate(resource, resourceId, operation)?.run {
                    relationUpdateProducer.publishRelationUpdate(this)
                    metricService.incrementRelationSuccess(resourceName)
                }
            }.onFailure { error ->
                val reason = error.getReason()
                metricService.incrementRelationFailure(resourceId, resourceName, reason)
                error.log(resourceName, resourceId, reason)
            }
        }
    }

    private fun Throwable.log(
        resourceName: String,
        resourceId: String,
        reason: MetricReason,
        messageOverride: String? = null,
    ) {
        val logMessage =
            messageOverride
                ?: "Failed to publish update for resource '$resourceName' ($resourceId). Reason: ${reason.tagValue}"

        if (this is AutoRelationException) {
            logger.error { "$logMessage. Error: ${this.message}" }
        } else {
            logger.error(this) { logMessage }
        }
    }

    private fun Throwable.getReason() =
        when (this) {
            is AutoRelationException -> metricReason
            else -> MetricReason.UNEXPECTED_ERROR
        }

    private fun fetchRules(resourceName: String) =
        relationRuleRegistry.getRules(
            domainName = consumerConfiguration.domain,
            packageName = consumerConfiguration.packageName,
            resourceName = resourceName,
        )
}
