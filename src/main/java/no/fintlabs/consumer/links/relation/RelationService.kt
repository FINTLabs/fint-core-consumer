package no.fintlabs.consumer.links.relation

import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service

@Service
class RelationService(
    private val unresolvedRelationCache: UnresolvedRelationCache,
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val relationRuleRegistry: RelationRuleRegistry,
    private val consumerConfig: ConsumerConfiguration,
) {
    fun processRelationUpdate(relationUpdate: RelationUpdate) {
    }
//        getResource(relationUpdate.resource.name, relationUpdate.resource.id)?.let { resource ->
//            relationUpdater.update(relationUpdate, resource)
//            linkService.mapLinks(relationUpdate.resource.name, resource)
//        } ?: registerLinksToBuffer(relationUpdate)

    fun handleLinks(
        resource: String,
        resourceId: String,
        resourceObject: FintResource,
    ) {
        // TODO: prune links & send relation updates
        // TODO: Attach persisted links
        // TODO: Attach buffered links
    }

    /**
     * Persists existing links by attaching them to the update object to prevent data loss.
     */
    private fun attachPreviousLinks(
        resource: String,
        resourceId: String,
        relation: String,
        resourceObject: FintResource,
    ) = getResource(resource, resourceId)
        ?.let { it.links[relation] }

    /**
     * Attaches relation links that is waiting on this specific resource.
     */
    private fun attachPolledLinks(
        resource: String,
        resourceId: String,
        relation: String,
        resourceObject: FintResource,
    ) = unresolvedRelationCache
        .takeRelations(resource, resourceId, relation)

    private fun getResource(
        resource: String,
        resourceId: String,
    ): FintResource? =
        cacheService
            .getCache(resource)
            ?.get(resourceId)
}
