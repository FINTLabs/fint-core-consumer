package no.fintlabs.autorelation.model

data class RelationEvent(
    val orgId: String,
    val sourceEntity: EntityDescriptor,
    val sourceId: String,
    val sourceData: Any,
    val operation: RelationOperation,
) {
    init {
        require(!orgId.contains("-")) { "OrgId must use dot instead of dash: $orgId" }
        require(orgId == orgId.lowercase()) { "OrgId must be lowercase: $orgId" }
    }
}

/**
 * Creates a [RelationEvent] for an ADD operation by parsing a FINT Kafka topic.
 *
 * @param key The record key, mapped to `sourceId`.
 * @param topic The Kafka topic (e.g., `"fintlabs-no.fint-core.entity.utdanning-vurdering-elevfravar"`).
 * @param value The record value, mapped to `sourceData`.
 */
fun createAddEvent(
    key: String,
    topic: String,
    value: Any,
) = topic.split(".").let { (orgId, _, _, resourceTypeString) ->
    RelationEvent(
        operation = RelationOperation.ADD,
        orgId = orgId.toDotFormat(),
        sourceEntity = resourceTypeString.toEntityDescriptor(),
        sourceId = key,
        sourceData = value,
    )
}

/**
 * Creates a [RelationEvent] for a DELETE operation using explicit entity details.
 */
fun createDeleteEvent(
    domainName: String,
    packageName: String,
    resourceName: String,
    orgId: String,
    resource: Any,
    resourceId: String,
) = RelationEvent(
    operation = RelationOperation.DELETE,
    orgId = orgId.toDotFormat(),
    sourceEntity = EntityDescriptor(domainName, packageName, resourceName),
    sourceId = resourceId,
    sourceData = resource,
)

private fun String.toDotFormat() = replace("-", ".")
