package no.fintlabs.consumer.kafka.entity

import no.fint.model.resource.FintResource
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.TOPIC_RETENTION_TIME
import no.fintlabs.consumer.kafka.long
import no.fintlabs.consumer.kafka.nullableLong
import org.apache.kafka.clients.consumer.ConsumerRecord

data class KafkaEntity(
    val key: String,
    val name: String,
    val resource: FintResource?,
    val lastModified: Long,
    val retentionTime: Long?, // TODO: CT-2350 Make this field non-nullable
    val sync: EntitySync,
)

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
