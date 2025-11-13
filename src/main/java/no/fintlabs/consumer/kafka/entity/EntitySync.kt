package no.fintlabs.consumer.kafka.entity

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.*
import no.fintlabs.consumer.kafka.byte
import no.fintlabs.consumer.kafka.long
import no.fintlabs.consumer.kafka.string
import org.apache.kafka.common.header.Headers

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

fun createEntitySync(headers: Headers): EntitySync? =
    runCatching {
        EntitySync(
            type = headers.syncType(),
            corrId = headers.string(SYNC_CORRELATION_ID),
            totalSize = headers.long(SYNC_TOTAL_SIZE),
        )
    }.getOrNull()

private fun Headers.syncType() =
    this
        .byte(SYNC_TYPE)
        .let { SyncType.entries[it.toInt()] }
