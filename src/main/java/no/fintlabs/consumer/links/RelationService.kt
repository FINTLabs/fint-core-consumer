package no.fintlabs.consumer.links

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationRef
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.ResourceId
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class RelationService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationCacheService: RelationCacheService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun processIfApplicable(relationUpdate: RelationUpdate) =
        relationUpdate.takeIf(::belongsToThisService)
            ?.let(::processRelationUpdate)

    fun processRelationUpdate(relationUpdate: RelationUpdate) =
        getResource(relationUpdate)
            ?.let { processRelation(relationUpdate, it) }
            ?: run { registerLinks(relationUpdate) }

    fun addRelations(resource: String, resourceId: String, resourceObject: FintResource) =
        resourceObject.links.forEach { (relationName, links) -> // Should we iterate over relations we shouldn't mutate? (worst case cache lookup O(1) ~200MB on 5 million relation lookups)
            mutateLinks(links, relationCacheService.pollLinks(resource, resourceId, relationName))
        }

    private fun registerLinks(relationUpdate: RelationUpdate) =
        relationCacheService.registerLinks(
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
        relationRef.ids.forEach { id ->
            links.removeIf {
                it.href.endsWith(formatIdLink(id), ignoreCase = true)
            }
        }

    private fun getResource(relationUpdate: RelationUpdate): FintResource? =
        cacheService.getCache(relationUpdate.resource.name)
            ?.get(relationUpdate.resource.id.value)

    private fun belongsToThisService(relationUpdate: RelationUpdate) =
        consumerConfig.matchesConfiguration(relationUpdate.domainName, relationUpdate.packageName, relationUpdate.orgId)

    private fun formatOrgId(orgId: String) =
        orgId.replace("-", ".")
            .replace("_", ".")

    private fun formatIdLink(id: ResourceId) = "${id.field}/${id.value}"

}