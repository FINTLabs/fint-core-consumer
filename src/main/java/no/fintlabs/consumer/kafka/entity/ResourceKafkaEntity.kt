package no.fintlabs.consumer.kafka.entity

import no.fint.model.resource.FintResource
import no.fintlabs.consumer.kafka.KafkaConstants.ENTITY_RETENTION_TIME
import no.fintlabs.consumer.kafka.KafkaHeader
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers

data class ResourceKafkaEntity(
    val key: String,
    val name: String,
    val resource: FintResource?,
    val createdTime: Long?
) {
    companion object {
        @JvmStatic
        fun from(resourceName: String, resource: FintResource?, record: ConsumerRecord<String, Any>) =
            ResourceKafkaEntity(
                name = resourceName,
                key = record.key(),
                resource = resource,
                createdTime = getCreatedTime(record.headers())
            )

        private fun getCreatedTime(headers: Headers) =
            headers.lastHeader(ENTITY_RETENTION_TIME)
                ?.let { KafkaHeader.getLong(it) }

    }
}