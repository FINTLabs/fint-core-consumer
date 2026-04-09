package no.fintlabs.consumer.kafka.entity

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_CORRELATION_ID
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TOTAL_SIZE
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TYPE
import no.fintlabs.consumer.kafka.byteValue
import no.fintlabs.consumer.kafka.longValue
import no.fintlabs.consumer.kafka.stringValue
import no.fintlabs.consumer.resource.ResourceConverter
import no.fintlabs.kafka.extractIdentifier
import no.novari.fint.model.resource.FintResource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers

/**
 * Holds the fields needed to cache a FINT resource.
 *
 * A null [resource] indicates a deletion.
 * [syncMetadata] is null when the resource originates from an event rather than a sync.
 */
data class ResourceMessage(
    val resourceName: String,
    val resourceId: String,
    val resource: FintResource?,
    val timestamp: Long,
    val syncMetadata: SyncMetadata?,
)

/**
 * Identifies which sync operation a resource belongs to, including its type,
 * correlation id, and the total number of resources in the sync.
 */
data class SyncMetadata(
    val corrId: String,
    val type: SyncType,
    val totalSize: Long,
)

fun ConsumerRecord<String, Any?>.toResourceMessage(resourceConverter: ResourceConverter): ResourceMessage {
    val resourceName =
        headers().stringValue(RESOURCE_NAME)
            ?: throw IllegalArgumentException("Resource name header not found")

    return ResourceMessage(
        resourceName = resourceName,
        resourceId = extractIdentifier(),
        resource = value()?.let { resourceConverter.convert(resourceName, it) },
        timestamp =
            headers().longValue(LAST_MODIFIED)
                ?: throw IllegalArgumentException("Required '$LAST_MODIFIED' header is missing"),
        syncMetadata = headers().extractSyncMetadata(),
    )
}

private fun Headers.extractSyncMetadata(): SyncMetadata? {
    val type = byteValue(SYNC_TYPE) ?: return null
    return SyncMetadata(
        type =
            SyncType.entries.getOrNull(type.toInt())
                ?: throw IllegalArgumentException("Invalid SyncType value: $type"),
        corrId =
            stringValue(SYNC_CORRELATION_ID)
                ?: throw IllegalArgumentException("Sync message missing correlation id"),
        totalSize =
            longValue(SYNC_TOTAL_SIZE)
                ?: throw IllegalArgumentException("Sync message missing total size"),
    )
}
