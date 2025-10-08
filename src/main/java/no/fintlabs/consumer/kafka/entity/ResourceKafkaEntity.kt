package no.fintlabs.consumer.kafka.entity

import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.*
import no.fintlabs.consumer.kafka.KafkaHeader
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers

open class ResourceKafkaEntity(
    val key: String,
    val name: String,
    val resource: FintResource?,
    val lastModified: Long,
    val syncType: SyncType,
    val syncCorrId: String
) {
    companion object {
        @JvmStatic
        fun from(resourceName: String, resource: FintResource?, record: ConsumerRecord<String, Any>) =
            ResourceKafkaEntity(
                name = resourceName,
                key = record.key(),
                resource = resource,
                lastModified = getLastModified(record.headers()),
                syncType = getSyncType(record.headers()),
                syncCorrId = getSyncCorrId(record.headers())
            )

        private fun getSyncCorrId(headers: Headers): String =
            headers.lastHeader(SYNC_CORRELATION_ID)
                ?.let { KafkaHeader.getString(it) }
                ?: throw IllegalArgumentException()

        private fun getSyncType(headers: Headers): SyncType =
            headers.lastHeader(SYNC_TYPE)
                ?.let { KafkaHeader.getByte(it) }
                ?.let { enumValues<SyncType>().getOrNull(it.toInt()) }
                ?: throw IllegalArgumentException()

        private fun getLastModified(headers: Headers): Long =
            headers.lastHeader(MODIFIED_TIME)
                ?.let { KafkaHeader.getLong(it) }
                ?: throw IllegalArgumentException()

    }
}