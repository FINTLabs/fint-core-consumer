package no.fintlabs.consumer.links

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationRef
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.ResourceId
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.entity.EntityProducer
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Service

@Service
class RelationService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val entityProducer: EntityProducer,
    private val publisher: ApplicationEventPublisher,
    private val consumerConfig: ConsumerConfiguration
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun processIfApplicable(relationUpdate: RelationUpdate) =
        relationUpdate.takeIf(::belongsToThisService)
            ?.let(::processRelationUpdate)

    fun processRelationUpdate(relationUpdate: RelationUpdate): Boolean =
        getResource(relationUpdate)
            ?.let { processRelation(relationUpdate, it) }
            ?: run {
                publisher.publishEvent(relationUpdate)
                false
            }

    private fun processRelation(relationUpdate: RelationUpdate, resource: FintResource): Boolean =
        resource.links
            ?.getOrPut(relationUpdate.relation.name) { mutableListOf() }
            ?.let { mutateRelation(relationUpdate, it) }
            ?.let {
                linkService.mapLinks(relationUpdate.resource.name, resource)
                entityProducer.produceEntity(relationUpdate, resource)
                true
            }
            ?: run {
                logger.error("Unable to process relation update")
                false
            }

    private fun mutateRelation(relationUpdate: RelationUpdate, links: MutableList<Link>) =
        when (relationUpdate.operation) {
            RelationOperation.ADD -> addRelations(links, relationUpdate.relation)
            RelationOperation.DELETE -> deleteRelations(links, relationUpdate.relation)
        }

    private fun addRelations(links: MutableList<Link>, relationRef: RelationRef) =
        relationRef.ids.forEach { id ->
            if (!links.any { linkExists(it, id) })
                links.add(Link.with(formatIdLink(id)))
        }

    private fun linkExists(link: Link, id: ResourceId) =
        link.toString().endsWith("${id.field}/${id.value}", ignoreCase = true)

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
        consumerConfig.orgId.equals(formatOrgId(relationUpdate.orgId))
                && consumerConfig.domain.equals(relationUpdate.domainName)
                && consumerConfig.packageName.equals(relationUpdate.packageName)

    private fun formatOrgId(orgId: String) =
        orgId.replace("-", ".")
            .replace("_", ".")

    private fun formatIdLink(id: ResourceId) = "${id.field}/${id.value}"

}