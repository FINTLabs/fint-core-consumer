package no.fintlabs.consumer.kafka.entity

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.*
import no.fintlabs.consumer.kafka.byteValue
import no.fintlabs.consumer.kafka.longValue
import no.fintlabs.consumer.kafka.stringValue
import org.apache.kafka.common.header.Headers

/**
 * Sync information for a resource event.
 *
 * Indicates whether it's a FULL, DELTA, or DELETE sync,
 * including its correlation ID and expected total size.
 */
data class ConsumerRecordMetadata(
    val type: SyncType,
    val corrId: String,
    val totalSize: Long,
)

fun createRecordMetadata(headers: Headers) =
    headers
        .byteValue(SYNC_TYPE)
        ?.let { syncTypeByte ->
            ConsumerRecordMetadata(
                type = syncType(syncTypeByte),
                corrId = headers.getCorrelationId(),
                totalSize = headers.getTotalSize(),
            )
        }

private fun Headers.getCorrelationId() =
    stringValue(SYNC_CORRELATION_ID)
        ?.takeIf { it.isNotBlank() }
        ?: throw IllegalArgumentException("corrId cannot be null")

private fun Headers.getTotalSize() =
    longValue(SYNC_TOTAL_SIZE)
        ?: throw IllegalArgumentException("totalSize cannot be null")

private fun syncType(syncTypeByte: Byte?) =
    syncTypeByte
        ?.let { SyncType.entries.getOrNull(it.toInt()) }
        ?: throw IllegalArgumentException("Invalid SyncType value: $syncTypeByte")
