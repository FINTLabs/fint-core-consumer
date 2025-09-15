package no.fintlabs.consumer.kafka.entity

import no.fint.model.resource.FintResource
import no.fintlabs.consumer.kafka.KafkaConstants.ENTITY_RETENTION_TIME
import no.fintlabs.consumer.kafka.KafkaConstants.IS_TRUE_STATE
import no.fintlabs.consumer.kafka.KafkaHeader
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers

data class KafkaEntity(
    val key: String,
    val name: String,
    val resource: FintResource?,
    val trueState: Boolean,
    val createdTime: Long?
) {
    companion object {
        fun from(resourceName: String, resource: FintResource?, record: ConsumerRecord<String, Any>) =
            KafkaEntity(
                name = resourceName,
                key = record.key(),
                resource = resource,
                trueState = getTrueState(record.headers()),
                createdTime = getCreatedTime(record.headers())
            )

        private fun getCreatedTime(headers: Headers) =
            headers.lastHeader(ENTITY_RETENTION_TIME)
                ?.let { KafkaHeader.getLong(it) }

        private fun getTrueState(headers: Headers) =
            headers.lastHeader(IS_TRUE_STATE) != null
    }
}