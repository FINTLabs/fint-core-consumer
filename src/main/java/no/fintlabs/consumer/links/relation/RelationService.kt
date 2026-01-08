package no.fintlabs.consumer.links.relation

import no.fint.model.resource.FintResource
import no.fintlabs.autorelation.cache.RelationCache
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
    private val relationCache: RelationCache,
    private val relationUpdater: RelationUpdater,
    private val consumerConfig: ConsumerConfiguration,
) {
    fun processRelationUpdate(relationUpdate: RelationUpdate) =
        getResource(relationUpdate.resource.name, relationUpdate.resource.id)?.let { resource ->
            relationUpdater.update(relationUpdate, resource)
            linkService.mapLinks(relationUpdate.resource.name, resource)
        } ?: cacheUnresolvedRelation(relationUpdate)

    fun handleLinks(
        resourceName: String,
        resourceId: String,
        resourceObject: FintResource,
    ) {
        getInverseRelationsForResource(resourceName).map { relation ->
            attachPreviousLinks(resourceName, resourceId, relation, resourceObject)
            attachPolledLinks(resourceName, resourceId, relation, resourceObject)
        }
    }

    /**
     * Persists existing links by attaching them to the update object to prevent data loss.
     */
    private fun attachPreviousLinks(
        resourceName: String,
        resourceId: String,
        relation: String,
        resourceObject: FintResource,
    ) = getResource(resourceName, resourceId)
        ?.let { it.links[relation] }
        ?.let { relationUpdater.addLinks(resourceObject, relation, it) }

    private fun getInverseRelationsForResource(resource: String) =
        relationCache.inverseRelationsForTarget(consumerConfig.domain, consumerConfig.packageName, resource)

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
        .let { relationUpdater.attachBuffered(resourceObject, relation, it) }

    private fun getResource(
        resourceName: String,
        resourceId: String,
    ): FintResource? =
        cacheService.getCache(resourceName).get(resourceId)

    private fun cacheUnresolvedRelation(relationUpdate: RelationUpdate) =
        unresolvedRelationCache.registerRelations(
            resource = relationUpdate.resource.name,
            resourceId = relationUpdate.resource.id,
            relation = relationUpdate.relation.name,
            relationLinks = relationUpdate.relation.links,
        )
}
