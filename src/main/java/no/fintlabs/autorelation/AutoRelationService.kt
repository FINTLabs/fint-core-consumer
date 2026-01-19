package no.fintlabs.autorelation

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceConverter
import no.novari.metamodel.MetamodelService
import org.springframework.stereotype.Service

@Service
class AutoRelationService(
    private val resourceConverter: ResourceConverter,
    private val consumerConfiguration: ConsumerConfiguration,
    private val metamodel: MetamodelService
) {
    fun handleNewEntity(
        resourceName: String,
        resourceId: String,
        resource: Any,
    ) {
        // If resource has no managedRelations, return early

        val convert = resourceConverter.convert(resourceName, resource) // TODO: Handle error

        // Get managed relations
        // Create RelationUpdate

        // Publish RelationUpdate to Kafka
    }
}
