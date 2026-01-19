package no.fintlabs.autorelation

import mu.KotlinLogging
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.kafka.RelationUpdateProducer
import no.fintlabs.autorelation.model.*
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceConverter
import no.novari.fint.model.resource.FintResource
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
            logger.error(error) { "Failed to convert resource '$resourceName' with ID '$resourceId'" }
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

                logger.error(error) {
                    "Failed to publish update for resource '$resourceName' ($resourceId). Reason: ${reason.tagValue}"
                }
            }
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
