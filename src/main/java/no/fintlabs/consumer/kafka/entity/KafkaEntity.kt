package no.fintlabs.consumer.kafka.entity

import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.*
import no.fintlabs.consumer.kafka.headerByteValue
import no.fintlabs.consumer.kafka.headerLongValue
import no.fintlabs.consumer.kafka.headerStringValue
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Represents a FINT entity consumer record.
 *
 * This class collects all relevant fields from the Kafka consumer record (key, headers,
 * resource payload, and optional sync metadata) so they can be handled as one
 * cohesive object instead of spreading raw Kafka details throughout the codebase.
 *
 * - `resource` is nullable: a `null` value indicates the entity is being deleted.
 * - `sync` is nullable: not all entities participate in sync operations.
 */
data class KafkaEntity(
    val key: String,
    val resourceName: String,
    val resource: FintResource?,
    val timestamp: Long,
    val type: SyncType?,
    val corrId: String?,
    val totalSize: Long?
) {
    companion object {

        /**
         * Creates a [KafkaEntity] from a Kafka record by extracting the key,
         * headers, resource payload, and optional sync metadata.
         */
        fun create(
            resourceName: String,
            resource: FintResource?,
            record: ConsumerRecord<String, Any>,
        ): KafkaEntity {
            val syncTypeByte = record.headerByteValue(SYNC_TYPE)

            return KafkaEntity(
                key = record.key(),
                resourceName = resourceName,
                resource = resource,
                timestamp = record.timestamp(),
                type = syncTypeByte?.let { syncType(syncTypeByte) },
                corrId = record.headerStringValue(SYNC_CORRELATION_ID),
                totalSize = record.headerLongValue(SYNC_TOTAL_SIZE)
            )
        }
    }
}

private fun syncType(value: Byte?) =
    value
        ?.let { SyncType.entries.getOrNull(it.toInt()) }
        ?: throw IllegalArgumentException("Invalid SyncType value: $value")