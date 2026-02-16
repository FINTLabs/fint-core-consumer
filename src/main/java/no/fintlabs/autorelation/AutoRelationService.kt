package no.fintlabs.autorelation

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationSyncRule
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.context.ResourceContext
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service

@Service
class AutoRelationService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationRuleRegistry: RelationRuleRegistry,
    private val relationEventService: RelationEventService,
    private val unresolvedRelationCache: UnresolvedRelationCache,
    private val resourceContext: ResourceContext,
    private val objectMapper: ObjectMapper,
) {
    fun applyOrBufferUpdate(relationUpdate: RelationUpdate) {
        relationUpdate.targetIds.forEach { id ->
            val resource = getResourceFromCache(relationUpdate.targetEntity.resourceName, id)

            if (resource != null) {
                val resourceCopy = resource.deepCopy(objectMapper, relationUpdate.getResourceClass())

                resourceCopy.applyUpdate(relationUpdate)
                linkService.mapLinks(relationUpdate.targetEntity.resourceName, resourceCopy)

                putInCache(relationUpdate, id, resourceCopy)
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
        val managedRules = getManagedRelations(resourceName)

        if (oldResource != null) {
            fintResource.pruneObsoleteLinks(resourceName, resourceId, oldResource, managedRules)
        }

        val managedRelationNames = managedRules.map { it.targetRelation }.toSet()

        relationRuleRegistry
            .getInverseRelations(consumerConfig.domain, consumerConfig.packageName, resourceName)
            .filter { it !in managedRelationNames } // Safety net
            .forEach { relation ->
                fintResource.preserveExistingLinks(oldResource, relation)
                fintResource.applyPendingLinks(resourceName, resourceId, relation)
            }
    }

    private fun getManagedRelations(resourceName: String) =
        relationRuleRegistry.getRules(consumerConfig.domain, consumerConfig.packageName, resourceName)

    private fun FintResource.pruneObsoleteLinks(
        resourceName: String,
        resourceId: String,
        oldResource: FintResource,
        managedRules: List<RelationSyncRule>,
    ) {
        val pruningRules = managedRules.filter { it.shouldPruneLinks() }
        if (pruningRules.isEmpty()) return

        val relationsToCheck = pruningRules.map { it.targetRelation }
        val obsoleteLinksMap = this.findObsoleteLinks(oldResource, relationsToCheck)

        if (obsoleteLinksMap.isNotEmpty()) {
            relationEventService.removeObsoleteRelations(
                resourceName,
                resourceId,
                this,
                obsoleteLinksMap,
                pruningRules,
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
                        timestamp,
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

    private fun RelationUpdate.retryIfResourceArrived(id: String) =
        getResourceFromCache(targetEntity.resourceName, id)
            ?.deepCopy(objectMapper, getResourceClass())
            ?.run {
                applyUpdate(this@retryIfResourceArrived)
                linkService.mapLinks(targetEntity.resourceName, this)
                putInCache(this@retryIfResourceArrived, id, this)
            }

    private fun getResourceFromCache(
        resource: String,
        resourceId: String,
    ): FintResource? =
        cacheService
            .getCache(resource)
            .get(resourceId)

    private fun putInCache(
        relationUpdate: RelationUpdate,
        id: String,
        resource: FintResource,
    ) = cacheService
        .getCache(relationUpdate.targetEntity.resourceName)
        .put(id, resource, relationUpdate.timestamp)

    // use !! to fail-fast if an unknown resource enters the system
    private fun RelationUpdate.getResourceClass() = resourceContext.getResource(targetEntity.resourceName)!!.clazz
}
