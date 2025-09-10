package no.fintlabs.consumer.kafka.entity

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.entity.EntityProducerFactory
import no.fintlabs.kafka.entity.EntityProducerRecord
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import org.apache.kafka.common.header.internals.RecordHeaders
import org.springframework.stereotype.Service

@Service
class EntityProducer(
    entityProducerFactory: EntityProducerFactory,
    private val consumerConfiguration: ConsumerConfiguration
) {

    private val entityProducer = entityProducerFactory.createProducer(Any::class.java)
    private val headers = createHeaders()

    fun produceEntity(resourceName: String, resource: Any, key: String) =
        entityProducer.send(
            EntityProducerRecord.builder<Any>()
                .topicNameParameters(createTopic(resourceName))
                .key(key)
                .value(resource)
                .headers(headers)
                .build()
        )

    private fun createHeaders(): RecordHeaders =
        RecordHeaders().apply {
            add("consumer", consumerConfiguration.id.toByteArray())
        }

    private fun createTopic(resourceName: String) =
        EntityTopicNameParameters.builder()
            .orgId(formatOrgId(consumerConfiguration.orgId))
            .domainContext("fint-core")
            .resource(formatResource(resourceName))
            .build()

    private fun formatResource(resourceName: String) =
        "${consumerConfiguration.domain}-${consumerConfiguration.packageName}-$resourceName"

    private fun formatOrgId(orgId: String) =
        orgId.replace(".", "-")
            .replace("_", "-")

}