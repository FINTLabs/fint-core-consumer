package no.fintlabs.consumer.links.relation

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.createDeleteEvent
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationEventProducer
import no.fintlabs.consumer.links.LinkService
import org.springframework.stereotype.Service

@Service
class RelationService(
    private val pendingRelationCache: UnresolvedRelationCache, // Renamed from unresolvedRelationCache
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val relationRuleRegistry: RelationRuleRegistry,
    private val consumerConfig: ConsumerConfiguration,
    private val relationEventProducer: RelationEventProducer,
) {
    /**
     * Tries to apply the update immediately. If the resource is missing,
     * buffers the update for later reconciliation.
     */
    fun applyOrBufferUpdate(relationUpdate: RelationUpdate) =
        relationUpdate
            .getResourceFromCache()
            ?.applyUpdate(relationUpdate)
            ?.run { linkService.mapLinks(relationUpdate.targetEntity.resourceName, this) }
            ?: relationUpdate.applyOrBufferRelations()

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
        linksToDelete.forEach { _ ->
            relationEventProducer.publish(
                createDeleteEvent(
                    domainName = consumerConfig.domain,
                    packageName = consumerConfig.packageName,
                    resourceName = resourceName,
                    orgId = consumerConfig.orgId,
                    resource = oldResource,
                    resourceId = resourceId,
                ),
            )
        }
    }

    private fun FintResource.preserveExistingLinks(
        oldResource: FintResource?,
        relation: String,
    ) = oldResource?.links?.get(relation)?.let { oldLinks ->
        addUniqueLinks(relation, oldLinks)
    }

    private fun FintResource.applyPendingLinks(
        resource: String,
        resourceId: String,
        relation: String,
    ) = pendingRelationCache
        .takeRelations(resource, resourceId, relation)
        .let { addUniqueLinks(relation, it) }

    /**
     * Buffers the update and performs a final cache check to handle race conditions.
     *
     * If the resource arrives while we are buffering, the second check ensures the
     * update is applied immediately, preventing it from getting stuck in the buffer.
     */
    private fun RelationUpdate.applyOrBufferRelations() {
        updatePendingCache()
        retryIfResourceArrived()
    }

    private fun RelationUpdate.updatePendingCache() =
        with(binding) {
            when (operation) {
                RelationOperation.ADD -> {
                    pendingRelationCache.registerRelation(
                        targetEntity.resourceName,
                        targetId,
                        relationName,
                        link,
                    )
                }

                RelationOperation.DELETE -> {
                    pendingRelationCache.removeRelation(
                        targetEntity.resourceName,
                        targetId,
                        relationName,
                        link,
                    )
                }
            }
        }

    private fun RelationUpdate.retryIfResourceArrived() =
        getResourceFromCache()?.apply {
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

    private fun RelationUpdate.getResourceFromCache() = getResourceFromCache(targetEntity.resourceName, targetId)
}
