package no.fintlabs.consumer.links.relation

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fintlabs.autorelation.cache.RelationCache
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationRef
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.ResourceId
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class RelationService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val relationCache: RelationCache,
    private val consumerConfig: ConsumerConfiguration,
    private val linkBuffer: LinkBuffer
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun processRelationUpdate(relationUpdate: RelationUpdate) =
        getResource(relationUpdate)
            ?.let { processRelation(relationUpdate, it) }
            ?: registerLinksToBuffer(relationUpdate)

    fun addRelations(resource: String, resourceId: String, resourceObject: FintResource) =
        relationCache.getControlledRelationsForTarget(consumerConfig.domain, consumerConfig.packageName, resource)
            .forEach { relation ->
                resourceObject.links.getOrPut(relation) { mutableListOf() }
                    .addAll(linkBuffer.pollLinks(resource, resourceId, relation))
            }

    private fun getResource(relationUpdate: RelationUpdate): FintResource? =
        cacheService.getCache(relationUpdate.resource.name)
            ?.get(relationUpdate.resource.id.value)

    private fun registerLinksToBuffer(relationUpdate: RelationUpdate) =
        linkBuffer.registerLinks(
            resource = relationUpdate.resource.name,
            resourceId = relationUpdate.resource.id.value,
            relation = relationUpdate.relation.name,
            links = createLinks(relationUpdate.relation)
        )

    private fun processRelation(relationUpdate: RelationUpdate, resource: FintResource) =
        resource.links
            ?.getOrPut(relationUpdate.relation.name) { mutableListOf() }
            ?.let { mutateRelation(relationUpdate, it) }
            ?.let { linkService.mapLinks(relationUpdate.resource.name, resource) }
            ?: run { logger.error("Unable to process relation update") }

    private fun mutateRelation(relationUpdate: RelationUpdate, links: MutableList<Link>) =
        when (relationUpdate.operation) {
            RelationOperation.ADD -> mutateLinks(links, createLinks(relationUpdate.relation))
            RelationOperation.DELETE -> deleteRelations(links, relationUpdate.relation)
        }

    private fun createLinks(relationRef: RelationRef): List<Link> =
        relationRef.ids.map { Link.with("${it.field}/${it.value}") }

    private fun mutateLinks(mutableLinks: MutableList<Link>, linksToAdd: List<Link>) =
        linksToAdd.forEach { link ->
            if (!mutableLinks.any { linkMatches(it, link) })
                mutableLinks.add(link)
        }

    private fun linkMatches(existingLink: Link, idLink: Link) =
        existingLink.href.endsWith(
            idLink.href,
            ignoreCase = true
        ) // TODO: Replace List<Link> with Set<Link> or improved FintLinks data structure

    private fun deleteRelations(links: MutableList<Link>, relationRef: RelationRef) =
        relationRef.ids.first { id ->
            links.removeIf { it.href.endsWith(id.formatIdLink(), ignoreCase = true) }
        }

    private fun ResourceId.formatIdLink() = "${this.field}/${this.value}"

}