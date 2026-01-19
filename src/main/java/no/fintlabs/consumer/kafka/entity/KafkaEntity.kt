package no.fintlabs.consumer.kafka.entity

import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.TOPIC_RETENTION_TIME
import no.fintlabs.consumer.kafka.longValue
import no.novari.fint.model.resource.FintResource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers

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
    // TODO: Add field to determine if the entity is related to a Sync or an Event
    val retentionTime: Long?, // TODO: CT-2350 Make this field non-nullable
    val consumerRecordMetadata: ConsumerRecordMetadata?,
)

/**
 * Creates a {@link KafkaEntity} from a Kafka record by extracting the key,
 * headers, resource payload, and optional sync metadata.
 */
fun createKafkaEntity(
    resourceName: String,
    resource: FintResource?,
    record: ConsumerRecord<String, Any?>,
): KafkaEntity =
    KafkaEntity(
        key = record.getRequiredKey(),
        resourceName = resourceName,
        resource = resource,
        lastModified = record.headers().lastModified(),
        retentionTime = record.headers().longValue(TOPIC_RETENTION_TIME),
        consumerRecordMetadata = createRecordMetadata(record.headers()),
    )

private fun ConsumerRecord<String, Any?>.getRequiredKey() = key() ?: throw IllegalArgumentException("Key is missing")

private fun Headers.lastModified() =
    longValue(LAST_MODIFIED)
        ?: throw IllegalArgumentException("Last modified timestamp is missing")
