package no.fintlabs.consumer.links

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fintlabs.autorelation.kafka.model.RelationOperation
import no.fintlabs.autorelation.kafka.model.RelationRef
import no.fintlabs.autorelation.kafka.model.RelationUpdate
import no.fintlabs.autorelation.kafka.model.ResourceId
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceService
import org.springframework.stereotype.Service
import kotlin.jvm.optionals.getOrNull

@Service
class RelationService(
    private val linkService: LinkService,
    private val resourceService: ResourceService,
    private val consumerConfig: ConsumerConfiguration
) {

    fun processIfApplicable(relationUpdate: RelationUpdate) =
        relationUpdate.takeIf(::belongsToThisService)
            ?.let(::processRelationUpdate)

    fun processRelationUpdate(relationUpdate: RelationUpdate) =
        getResource(relationUpdate)
            ?.let { processRelation(relationUpdate, it) }
            ?: println("resource not found")

    private fun processRelation(relationUpdate: RelationUpdate, resource: FintResource): Unit =
        resource.linksIfPresent
            ?.getOrPut(relationUpdate.relation.name) { mutableListOf() }
            ?.let { mutateRelation(relationUpdate, it) }
            ?.let { linkService.mapLinks(relationUpdate.resource.name, resource) }
            ?: println("Relation not found")

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
        resourceService.getResourceById(
            relationUpdate.resource.name,
            relationUpdate.resource.id.field,
            relationUpdate.resource.id.value
        ).getOrNull()

    private fun belongsToThisService(relationUpdate: RelationUpdate) =
        consumerConfig.orgId.equals(formatOrgId(relationUpdate.orgId), ignoreCase = true)
                && consumerConfig.domain.equals(relationUpdate.domainName, ignoreCase = true)
                && consumerConfig.packageName.equals(relationUpdate.packageName, ignoreCase = true)

    private fun formatOrgId(orgId: String) =
        orgId.replace("-", ".")
            .replace("_", ".")

    private fun formatIdLink(id: ResourceId) = "${id.field}/${id.value}"

}