package no.fintlabs.autorelation.model

import no.novari.fint.model.FintIdentifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link

data class RelationBinding(
    val relationName: String,
    val link: Link,
)

fun RelationSyncRule.toRelationBinding(
    resource: FintResource,
    resourceId: String,
) = RelationBinding(
    relationName = inverseRelation,
    link = resource.toLink(resourceId),
)

private fun FintResource.toLink(resourceId: String): Link =
    identifikators
        .filter { it.value.notNullOrEmpty() }
        .entries
        .firstOrNull { it.value.identifikatorverdi == resourceId }
        ?.let { (idField, idValue) -> Link.with("$idField/$idValue") }
        ?: throw ResourceIdMismatchException(resourceId)

private fun FintIdentifikator?.notNullOrEmpty() = this != null && this.identifikatorverdi.isNotBlank()
