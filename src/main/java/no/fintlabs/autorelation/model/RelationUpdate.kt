package no.fintlabs.autorelation.model

import no.novari.fint.model.resource.FintResource

/**
 * Represents a request to modify a relation on a specific resource.
 *
 * This class carries all necessary context to locate a target resource within a specific
 * service (consumer) and apply a relation change (add or remove).
 *
 * @property orgId The unique identifier of the organization.
 * Format is always lowercase with a dot separator (e.g., "fintlabs.no").
 *
 * @property targetEntity Metadata defining the target resource's location.
 * Contains the domainName, packageName, and resourceName used to route this request
 * to the correct consumer service.
 *
 * @property targetId The unique identifier for the specific target resource.
 * Used to look up the resource (typically from cache) before applying the update.
 *
 * @property binding The relation data [RelationBinding] containing the field name and link
 * involved in this update.
 *
 * @property operation The action to perform on the target resource (e.g., ADD or REMOVE the [binding]).
 */
data class RelationUpdate(
    val targetEntity: EntityDescriptor,
    val targetId: String,
    val binding: RelationBinding,
    val operation: RelationOperation,
)

fun RelationSyncRule.toRelationUpdate(
    resource: FintResource,
    operation: RelationOperation,
): RelationUpdate? {
    val targetId = getTargetId(resource) ?: return null

    return RelationUpdate(
        targetEntity = targetType,
        targetId = targetId,
        binding = toRelationBinding(resource),
        operation = operation,
    )
}

// Fraværsregistrering kommer inn med en lenke til elevfravær 1-1

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
