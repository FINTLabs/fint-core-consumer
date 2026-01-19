package no.fintlabs.autorelation.model

import no.novari.fint.model.resource.FintResource

data class RelationUpdate(
    val targetEntity: EntityDescriptor,
    val targetId: String,
    val binding: RelationBinding,
    val operation: RelationOperation,
)

fun RelationSyncRule.toRelationUpdate(
    resource: FintResource,
    resourceId: String,
    operation: RelationOperation,
): RelationUpdate? {
    val targetId = getTargetId(resource) ?: return null

    return RelationUpdate(
        targetEntity = targetType,
        targetId = targetId,
        binding = toRelationBinding(resource, resourceId),
        operation = operation,
    )
}

private fun RelationSyncRule.getTargetId(resource: FintResource): String? {
    val href = resource.links[targetRelation]?.firstOrNull()?.href

    if (href.isNullOrBlank()) {
        return if (isMandatory) throw MissingMandatoryLinkException(targetRelation) else null
    }

    return href
        .split("/")
        .takeLast(2)
        .takeIf { segments -> segments.size > 1 && segments.all { it.isNotBlank() } }
        ?.last()
        ?: throw InvalidLinkException("Invalid link format for relation '$targetRelation': '$href'. Could not extract valid ID segments.")
}
