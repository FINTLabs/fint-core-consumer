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
        } ?: registerLinksToBuffer(relationUpdate)

    fun handleLinks(
        resource: String,
        resourceId: String,
        resourceObject: FintResource,
    ) {
        getInverseRelationsForResource(resource).map { relation ->
            attachPreviousLinks(resource, resourceId, relation, resourceObject)
            attachPolledLinks(resource, resourceId, relation, resourceObject)
        }
    }

    private fun attachPreviousLinks(
        resource: String,
        resourceId: String,
        relation: String,
        resourceObject: FintResource,
    ) = getResource(resource, resourceId)
        ?.let { it.links[relation] }
        ?.let { relationUpdater.addLinks(resourceObject, relation, it) }

    private fun getInverseRelationsForResource(resource: String) =
        relationCache.inverseRelationsForTarget(consumerConfig.domain, consumerConfig.packageName, resource)

    private fun attachPolledLinks(
        resource: String,
        resourceId: String,
        relation: String,
        resourceObject: FintResource,
    ) = unresolvedRelationCache
        .pollLinks(resource, resourceId, relation)
        .let { relationUpdater.attachBuffered(resourceObject, relation, it) }

    private fun getResource(
        resource: String,
        resourceId: String,
    ): FintResource? =
        cacheService
            .getCache(resource)
            ?.get(resourceId)

    private fun registerLinksToBuffer(relationUpdate: RelationUpdate) =
        unresolvedRelationCache.registerLinks(
            resource = relationUpdate.resource.name,
            resourceId = relationUpdate.resource.id,
            relation = relationUpdate.relation.name,
            links = relationUpdate.relation.links,
        )
}
