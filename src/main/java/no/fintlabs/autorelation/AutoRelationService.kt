package no.fintlabs.autorelation

import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceConverter
import org.springframework.stereotype.Service

@Service
class AutoRelationService(
    private val resourceConverter: ResourceConverter,
    private val consumerConfiguration: ConsumerConfiguration,
    private val relationRuleRegistry: RelationRuleRegistry,
) {
    fun handleNewEntity(
        resourceName: String,
        resourceId: String,
        resource: Any,
    ) {
        val rules = getRelationRules(resourceName)
        if (rules.isEmpty()) return

        val convert = resourceConverter.convert(resourceName, resource) // TODO: Handle error

        // Get managed relations
        // Create RelationUpdate

        // Publish RelationUpdate to Kafka
    }

    private fun getRelationRules(resourceName: String) =
        relationRuleRegistry.getRules(consumerConfiguration.domain, consumerConfiguration.packageName, resourceName)
}
