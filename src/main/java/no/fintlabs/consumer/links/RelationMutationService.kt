package no.fintlabs.consumer.links

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fintlabs.autorelation.cache.RelationCache
import no.fintlabs.consumer.config.ConsumerConfiguration
import org.springframework.stereotype.Service

@Service
class RelationMutationService(
    consumerConfig: ConsumerConfiguration,
    relationCache: RelationCache
) {

    private val resourceRelations =
        relationCache.getResourceRelations(consumerConfig.domain, consumerConfig.packageName)

    fun isControlled(resourceName: String): Boolean = resourceRelations.contains(resourceName)

    fun mutateNewEntity(resourceName: String, resource: FintResource, previousEntity: FintResource) =
        resourceRelations[resourceName]?.forEach { relationName ->
            getPreviousLinks(previousEntity, relationName)
                ?.let { links -> addPreviousLinksToRelation(relationName, links, resource) }
        }

    private fun getPreviousLinks(previousEntity: FintResource, relationName: String) =
        previousEntity.linksIfPresent?.get(relationName)

    private fun addPreviousLinksToRelation(relationName: String, previousLinks: List<Link>, resource: FintResource) =
        resource.linksIfPresent
            ?.getOrPut(relationName) { mutableListOf() }
            ?.let { links ->
                links.clear()
                links.addAll(previousLinks)
            }

}