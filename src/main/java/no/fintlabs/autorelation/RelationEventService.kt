package no.fintlabs.autorelation

import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.kafka.RelationUpdateProducer
import no.fintlabs.autorelation.model.AutoRelationException
import no.fintlabs.autorelation.model.MetricReason
import no.fintlabs.autorelation.model.toEmptyRelationState
import no.fintlabs.autorelation.model.toRelationState
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.fint.model.resource.FintResource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class RelationEventService(
    private val relationRuleRegistry: RelationRuleRegistry,
    private val consumerConfiguration: ConsumerConfiguration,
    private val relationUpdateProducer: RelationUpdateProducer,
    private val metricService: MetricService,
) {
    /**
     * Publish the current relation state for a source resource: for each managed rule, the full set
     * of targets the source currently links to. Consumers diff this against what they hold.
     */
    fun publishState(
        resourceName: String,
        resourceId: String,
        resource: FintResource,
    ) {
        val rules = fetchRules(resourceName).ifEmpty { return }
        rules.forEach { rule ->
            publish(resourceName, resourceId, rule.targetRelation) {
                relationUpdateProducer.publish(rule.toRelationState(resource, resourceId), resourceName, resourceId)
            }
        }
    }

    /**
     * Publish empty state for every managed rule of a removed source, so consumers drop the
     * back-links pointing to it.
     */
    fun publishRemoval(
        resourceName: String,
        resourceId: String,
        resource: FintResource,
    ) {
        val rules = fetchRules(resourceName).ifEmpty { return }
        rules.forEach { rule ->
            publish(resourceName, resourceId, rule.targetRelation) {
                relationUpdateProducer.publish(
                    rule.toEmptyRelationState(resource, resourceId),
                    resourceName,
                    resourceId,
                )
            }
        }
    }

    private fun publish(
        resourceName: String,
        resourceId: String,
        relationName: String? = null,
        block: () -> Unit,
    ) = runCatching(block)
        .onFailure { error ->
            val reason = error.toMetricReason()
            metricService.incrementRuleSkipped(resourceName, reason)
            logRelationError(error, resourceName, resourceId, reason, relationName)
        }

    private fun logRelationError(
        error: Throwable,
        resourceName: String,
        resourceId: String,
        reason: MetricReason,
        relationName: String? = null,
    ) {
        val context = relationName?.let { " Relation: $it" } ?: ""
        val msg = "Failed to publish state for '$resourceName' ($resourceId). Reason: ${reason.tagValue}.$context"

        if (error is AutoRelationException) {
            logger.error("{} Error: {}", msg, error.message)
        } else {
            logger.error("{}", error.message)
        }
    }

    private fun Throwable.toMetricReason() =
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

    companion object {
        private val logger = LoggerFactory.getLogger(RelationEventService::class.java)
    }
}
