package no.fintlabs.autorelation

import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import org.springframework.stereotype.Service

@Service
class AutoRelationService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationRuleRegistry: RelationRuleRegistry,
    private val relationEventService: RelationEventService,
    private val unresolvedRelationCache: UnresolvedRelationCache,
) {
    fun applyOrBufferUpdate(relationUpdate: RelationUpdate) {
        relationUpdate.targetIds.forEach { id ->
            val resource = getResourceFromCache(relationUpdate.targetEntity.resourceName, id)

            if (resource != null) {
                resource.applyUpdate(relationUpdate)
                linkService.mapLinks(relationUpdate.targetEntity.resourceName, resource)
            } else {
                relationUpdate.applyOrBufferRelation(id)
            }
        }
    }

    /**
     * Main reconciliation entry point.
     * Handles Pruning (removals), Preservation (old links), and Hydration (pending links).
     */
    fun reconcileLinks(
        resourceName: String,
        resourceId: String,
        fintResource: FintResource,
    ) {
        val oldResource = getResourceFromCache(resourceName, resourceId)

        if (oldResource != null) {
            fintResource.pruneObsoleteLinks(resourceName, resourceId, oldResource)
        }

        relationRuleRegistry
            .getInverseRelations(consumerConfig.domain, consumerConfig.packageName, resourceName)
            .takeIf { it.isNotEmpty() }
            ?.forEach { relation ->
                fintResource.preserveExistingLinks(oldResource, relation)
                fintResource.applyPendingLinks(resourceName, resourceId, relation)
            }
    }

    private fun FintResource.pruneObsoleteLinks(
        resourceName: String,
        resourceId: String,
        oldResource: FintResource,
    ) {
        val managedRelations = getManagedRelations(resourceName)
        if (managedRelations.isEmpty()) return

        this
            .findObsoleteLinks(oldResource, managedRelations)
            .publishDeletionEvents(resourceName, resourceId, oldResource)
    }

    private fun getManagedRelations(resourceName: String) =
        relationRuleRegistry
            .getRules(consumerConfig.domain, consumerConfig.packageName, resourceName)
            .map { it.targetRelation }

    private fun Map<String, List<Link>>.publishDeletionEvents(
        resourceName: String,
        resourceId: String,
        oldResource: FintResource,
    ) = this.forEach { (_, linksToDelete) ->
        linksToDelete.forEach { link ->
            // TODO: Bug fix tomorrow
            relationEventService.removeRelations(
                resourceName = resourceName,
                resourceId = resourceId,
                resource = oldResource,
            )
        }
    }

    private fun FintResource.preserveExistingLinks(
        oldResource: FintResource?,
        relation: String,
    ) = oldResource?.links?.get(relation)?.let { oldLinks ->
        this.addUniqueLinks(relation, oldLinks)
    }

    private fun FintResource.applyPendingLinks(
        resource: String,
        resourceId: String,
        relation: String,
    ) = unresolvedRelationCache
        .takeRelations(resource, resourceId, relation)
        .let { addUniqueLinks(relation, it) }

    private fun RelationUpdate.applyOrBufferRelation(id: String) {
        updatePendingCache(id)
        retryIfResourceArrived(id)
    }

    private fun RelationUpdate.updatePendingCache(id: String) =
        with(binding) {
            when (operation) {
                RelationOperation.ADD -> {
                    unresolvedRelationCache.registerRelation(
                        targetEntity.resourceName,
                        id,
                        relationName,
                        link,
                    )
                }

                RelationOperation.DELETE -> {
                    unresolvedRelationCache.removeRelation(
                        targetEntity.resourceName,
                        id,
                        relationName,
                        link,
                    )
                }
            }
        }

    // 4. UPDATED TO RETRY SPECIFIC ID
    private fun RelationUpdate.retryIfResourceArrived(id: String) =
        getResourceFromCache(targetEntity.resourceName, id)?.apply {
            applyUpdate(this@retryIfResourceArrived)
            linkService.mapLinks(targetEntity.resourceName, this)
        }

    private fun getResourceFromCache(
        resource: String,
        resourceId: String,
    ): FintResource? =
        cacheService
            .getCache(resource)
            ?.get(resourceId)
}
