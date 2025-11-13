package no.fintlabs.consumer.kafka.entity

import no.fintlabs.adapter.models.sync.SyncType

/**
 * Sync information for a resource event.
 *
 * Indicates whether it's a FULL, DELTA, or DELETE sync,
 * including its correlation ID and expected total size.
 */
data class EntitySync(
    val type: SyncType,
    val corrId: String,
    val totalSize: Long,
)

fun createEntitySync(syncType: Byte?, corrId: String?, totalSize: Long?): EntitySync =
    EntitySync(
        type = syncType(syncType),
        corrId = corrId?.takeIf { it.isNotBlank() } ?: throw java.lang.IllegalArgumentException("corrId cannot be null"),
        totalSize = totalSize ?: throw java.lang.IllegalArgumentException("totalSize cannot be null"),
    )

private fun syncType(value: Byte?) =
    value
        ?.let { SyncType.entries.getOrNull(it.toInt()) }
        ?: throw IllegalArgumentException("Invalid SyncType value: $value")
