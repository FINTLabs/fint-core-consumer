package no.fintlabs.consumer.links.relation

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationRef
import no.fintlabs.autorelation.model.RelationUpdate
import org.springframework.stereotype.Component

@Component
class RelationUpdater {

    fun update(relationUpdate: RelationUpdate, resource: FintResource) =
        getRelationLinks(relationUpdate.relation.name, resource)
            ?.let { mutateRelation(relationUpdate, it) }

    fun attachBuffered(resource: FintResource, relationName: String, linksToAttach: List<Link>) =
        takeIf { linksToAttach.isNotEmpty() }
            ?.let { getRelationLinks(relationName, resource) }
            ?.let { addLinks(it, linksToAttach) }

    private fun getRelationLinks(relation: String, resource: FintResource) =
        resource.links?.getOrPut(relation) { mutableListOf() }

    private fun mutateRelation(relationUpdate: RelationUpdate, links: MutableList<Link>) =
        when (relationUpdate.operation) {
            RelationOperation.ADD -> mutateLinks(links, relationUpdate.relation.links)
            RelationOperation.DELETE -> deleteRelations(links, relationUpdate.relation)
        }

    private fun mutateLinks(mutableLinks: MutableList<Link>, linksToAdd: List<Link>) =
        linksToAdd.forEach { link ->
            if (!mutableLinks.any { linkMatches(it, link) })
                mutableLinks.add(link)
        }

    private fun addLinks(target: MutableList<Link>, toAdd: List<Link>) {
        toAdd.forEach { link ->
            if (target.none { linkMatches(it, link) }) target.add(link)
        }
    }

    private fun linkMatches(existingLink: Link, idLink: Link) =
        existingLink.href.endsWith(
            idLink.href,
            ignoreCase = true
        ) // TODO: Replace List<Link> with Set<Link> or improved FintLinks data structure

    private fun deleteRelations(links: MutableList<Link>, relationRef: RelationRef) =
        relationRef.links.first { id ->
            links.removeIf { linkMatches(it, id) }
        }

}