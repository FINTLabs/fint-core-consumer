package no.fintlabs.autorelation

import mu.KotlinLogging
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.kafka.RelationUpdateProducer
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationSyncRule
import no.fintlabs.autorelation.model.toRelationUpdate
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
            publishUpdates(rules, convertedResource, resourceId, RelationOperation.ADD)
        }.onFailure { error ->
            logger.error(error) { "Failed to convert resource '$resourceName' with ID '$resourceId'" }
        }
    }

    fun removeRelations(
        resourceName: String,
        resourceId: String,
        resource: FintResource,
    ) = fetchRules(resourceName)
        .takeIf { it.isNotEmpty() }
        ?.run { publishUpdates(this, resource, resourceId, RelationOperation.DELETE) }

    private fun publishUpdates(
        rules: List<RelationSyncRule>,
        resource: FintResource,
        resourceId: String,
        operation: RelationOperation,
    ) = rules
        .asSequence()
        .mapNotNull { rule -> rule.toRelationUpdate(resource, resourceId, operation) }
        .forEach { update -> relationUpdateProducer.publishRelationUpdate(update) }

    private fun fetchRules(resourceName: String) =
        relationRuleRegistry.getRules(
            domainName = consumerConfiguration.domain,
            packageName = consumerConfiguration.packageName,
            resourceName = resourceName,
        )
}
