package no.fintlabs.consumer.kafka.entity

import no.fint.model.resource.FintResource
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.long
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers

data class KafkaEntity(
    val key: String,
    val name: String,
    val resource: FintResource?,
    val lastModified: Long?,
)

fun createResourceKafkaEntity(
    resourceName: String,
    resource: FintResource?,
    record: ConsumerRecord<String, Any>,
) = ResourceKafkaEntity(
    name = resourceName,
    key = record.key(),
    resource = resource,
    lastModified = record.headers().long(LAST_MODIFIED),
    sync = createEntitySync(record.headers()),
)
