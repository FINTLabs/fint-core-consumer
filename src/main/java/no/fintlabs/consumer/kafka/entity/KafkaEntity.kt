package no.fintlabs.consumer.kafka.entity

import no.fint.model.resource.FintResource
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.TOPIC_RETENTION_TIME
import no.fintlabs.consumer.kafka.long
import no.fintlabs.consumer.kafka.nullableLong
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
    val name: String,
    val resource: FintResource?,
    val lastModified: Long,
    val retentionTime: Long?, // TODO: CT-2350 Make this field non-nullable
    val sync: EntitySync?,
)

/**
 * Creates a {@link KafkaEntity} from a Kafka record by extracting the key,
 * headers, resource payload, and optional sync metadata.
 */
fun createKafkaEntity(
    resourceName: String,
    resource: FintResource?,
    record: ConsumerRecord<String, Any>,
) = KafkaEntity(
    name = resourceName,
    key = record.key(),
    resource = resource,
    lastModified = record.headers().long(LAST_MODIFIED),
    retentionTime = record.headers().nullableLong(TOPIC_RETENTION_TIME),
    sync = createEntitySync(record.headers()),
)
