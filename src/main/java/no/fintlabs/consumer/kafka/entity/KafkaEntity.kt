package no.fintlabs.consumer.kafka.entity

import no.fint.model.resource.FintResource
import no.fintlabs.consumer.kafka.KafkaConstants.*
import no.fintlabs.consumer.kafka.headerByteValue
import no.fintlabs.consumer.kafka.headerLongValue
import no.fintlabs.consumer.kafka.headerStringValue
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Represents a single FINT entity received from Kafka.
 *
 * This class collects all relevant fields from the Kafka record (key, headers,
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
    val lastModified: Long,
    val retentionTime: Long?, // TODO: CT-2350 Make this field non-nullable
    val consumerRecordMetadata: ConsumerRecordMetadata?
)

/**
 * Creates a {@link KafkaEntity} from a Kafka record by extracting the key,
 * headers, resource payload, and optional sync metadata.
 */
fun createKafkaEntity(
    resourceName: String,
    resource: FintResource?,
    record: ConsumerRecord<String, Any>,
): KafkaEntity {
    val consumerRecordMetadata = ConsumerRecordMetadata.create(
        record.headerByteValue(SYNC_TYPE) ?: throw IllegalArgumentException("Sync type not found"),
        record.headerStringValue(SYNC_CORRELATION_ID) ?: throw IllegalArgumentException("Sync correlation ID not found"),
        record.headerLongValue(SYNC_TOTAL_SIZE) ?: throw IllegalArgumentException("Sync total size not found")
    )
    return KafkaEntity(
        key = record.key(),
        resourceName = resourceName,
        resource = resource,
        lastModified = record.headerLongValue(LAST_MODIFIED) ?: throw IllegalArgumentException("Last modified timestamp is missing"),
        retentionTime = record.headerLongValue(TOPIC_RETENTION_TIME),
        consumerRecordMetadata = consumerRecordMetadata
    )
}
