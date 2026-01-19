package no.fintlabs.autorelation.model

import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link

/**
 * @property relationName The name of the relation to bind to.
 * @property links The relation links that are getting bound.
 */
data class RelationBinding(
    val relationName: String,
    val links: List<Link>,
)

fun RelationSyncRule.toRelationBinding(resource: FintResource) =
    RelationBinding(
        relationName = inverseRelation,
        links = resource.toLinks(targetRelation),
    )

private fun FintResource.toLinks(relationName: String): List<Link> =
    this.links[relationName]
        ?.map { it.formatToRelativeLink() }
        ?: emptyList()

private fun Link.formatToRelativeLink(): Link =
    href
        .split("/")
        .takeIf { it.size > 1 }
        ?.takeLast(2)
        ?.let { Link.with(it.joinToString("/")) }
        ?: throw InvalidLinkException("Invalid link format: '$href'. Could not extract valid ID segments.")
