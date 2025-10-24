package no.fintlabs.consumer.kafka.entity

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.*
import no.fintlabs.consumer.kafka.byte
import no.fintlabs.consumer.kafka.long
import no.fintlabs.consumer.kafka.string
import org.apache.kafka.common.header.Headers

data class EntitySync(
    val type: SyncType,
    val corrId: String,
    val totalSize: Long
)

fun createEntitySync(headers: Headers): EntitySync =
    EntitySync(
        type = headers.syncType(),
        corrId = headers.string(SYNC_CORRELATION_ID),
        totalSize = headers.long(SYNC_TOTAL_SIZE)
    )

private fun Headers.syncType() =
    this.byte(SYNC_TYPE)
        .let { SyncType.entries.getOrNull(it.toInt()) }
        ?: throw IllegalArgumentException("Invalid SyncType index")
