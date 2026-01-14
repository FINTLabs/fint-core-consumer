package no.fintlabs.consumer.links.relation

import no.fint.model.resource.FintResource
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import org.springframework.stereotype.Service

@Service
class RelationService(
    private val unresolvedRelationCache: UnresolvedRelationCache,
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val relationRuleRegistry: RelationRuleRegistry,
    private val consumerConfig: ConsumerConfiguration,
) {
    fun processRelationUpdate(relationUpdate: RelationUpdate) =
        relationUpdate
            .getResourceFromCache()
            ?.applyUpdate(relationUpdate)
            ?.run { linkService.mapLinks(relationUpdate.targetEntity.resourceName, this) }
            ?: relationUpdate.registerLinksToBuffer()

    /**
     * Populates the [fintResource] with links by restoring historical data and resolving pending relations.
     *
     * This function iterates over registered inverse relations to:
     * 1. **Restore Persistence:** Re-attach previously known links from the cache to ensure existing relations are not lost.
     * 2. **Resolve Awaiting:** Poll for and attach links from other resources that were effectively "waiting" for this resource to arrive in the service.
     */
    fun attachRelations(
        resourceName: String,
        resourceId: String,
        fintResource: FintResource,
    ) = relationRuleRegistry
        .getInverseRelations(consumerConfig.domain, consumerConfig.packageName, resourceName)
        .forEach { relation ->
            attachPreviousLinks(resourceName, resourceId, relation, fintResource)
            attachUnresolvedRelations(resourceName, resourceId, relation, fintResource)
        }

    /**
     * Persists existing links by attaching them to the update object to prevent data loss.
     */
    private fun attachPreviousLinks(
        resource: String,
        resourceId: String,
        relation: String,
        fintResource: FintResource,
    ) = getResourceFromCache(resource, resourceId)
        ?.let { it.links[relation] }
        ?.let { fintResource.addUniqueLinks(relation, it) }

    private fun attachUnresolvedRelations(
        resource: String,
        resourceId: String,
        relation: String,
        fintResource: FintResource,
    ) = unresolvedRelationCache
        .takeRelations(resource, resourceId, relation)
        .let { fintResource.addUniqueLinks(relation, it) }

    private fun RelationUpdate.getResourceFromCache() = getResourceFromCache(targetEntity.resourceName, targetId)

    private fun getResourceFromCache(
        resource: String,
        resourceId: String,
    ): FintResource? =
        cacheService
            .getCache(resource)
            ?.get(resourceId)

    private fun RelationUpdate.registerLinksToBuffer() =
        unresolvedRelationCache.registerRelations(
            resource = targetEntity.resourceName,
            resourceId = targetId,
            relation = binding.relationName,
            relationLinks = binding.link,
        )
}
