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
    private val linkBuffer: LinkBuffer,
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val relationCache: RelationCache,
    private val relationUpdater: RelationUpdater,
    private val consumerConfig: ConsumerConfiguration
) {

    fun processRelationUpdate(relationUpdate: RelationUpdate) =
        getResource(relationUpdate)
            ?.let { updateAndMapLinks(relationUpdate, it) }
            ?: registerLinksToBuffer(relationUpdate)

    // TODO: Consider moving to own LinkBufferService
    fun attachBufferedRelations(resource: String, resourceId: String, resourceObject: FintResource) =
        getControlledRelations(resource)
            .map { attachPolledLinks(resource, resourceId, it, resourceObject) }

    private fun attachPolledLinks(
        resource: String,
        resourceId: String,
        relation: String,
        resourceObject: FintResource
    ) = linkBuffer.pollLinks(resource, resourceId, relation)
        .let { relationUpdater.attachBuffered(resourceObject, relation, it) }

    private fun getControlledRelations(resource: String) =
        relationCache.getControlledRelationsForTarget(consumerConfig.domain, consumerConfig.packageName, resource)

    private fun updateAndMapLinks(relationUpdate: RelationUpdate, resource: FintResource) =
        relationUpdater.update(relationUpdate, resource)
            .also { linkService.mapLinks(relationUpdate.resource.name, resource) }

    private fun getResource(relationUpdate: RelationUpdate): FintResource? =
        cacheService.getCache(relationUpdate.resource.name)
            ?.get(relationUpdate.resource.id.value)

    private fun registerLinksToBuffer(relationUpdate: RelationUpdate) =
        linkBuffer.registerLinks(
            resource = relationUpdate.resource.name,
            resourceId = relationUpdate.resource.id.value,
            relation = relationUpdate.relation.name,
            links = relationUpdate.relation.createLinks()
        )

}