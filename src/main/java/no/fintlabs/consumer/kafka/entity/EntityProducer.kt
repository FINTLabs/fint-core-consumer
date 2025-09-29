package no.fintlabs.consumer.kafka.entity

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaConstants.CONSUMER
import no.fintlabs.consumer.kafka.KafkaConstants.ENTITY_RETENTION_TIME
import no.fintlabs.kafka.entity.EntityProducerFactory
import no.fintlabs.kafka.entity.EntityProducerRecord
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import org.apache.kafka.common.header.internals.RecordHeaders
import org.springframework.stereotype.Service
import java.nio.ByteBuffer

@Service
class EntityProducer(
    entityProducerFactory: EntityProducerFactory,
    private val consumerConfiguration: ConsumerConfiguration
) {

    private val entityProducer = entityProducerFactory.createProducer(Any::class.java)

    fun produceEntity(relationUpdate: RelationUpdate, resource: Any) =
        entityProducer.send(
            EntityProducerRecord.builder<Any>()
                .topicNameParameters(createTopic(relationUpdate.resource.name))
                .key(relationUpdate.resource.id.value)
                .value(resource)
                .headers(createHeaders(relationUpdate.entityRetentionTime))
                .build()
        )

    fun produceEntity(kafkaEntity: KafkaEntity) =
        entityProducer.send(
            EntityProducerRecord.builder<Any>()
                .topicNameParameters(createTopic(kafkaEntity.name))
                .key(kafkaEntity.key)
                .value(kafkaEntity.resource)
                .headers(createHeaders(kafkaEntity.createdTime))
                .build()
        )

    private fun createHeaders(entityRetentionTime: Long?): RecordHeaders =
        RecordHeaders().apply {
            add(CONSUMER, consumerConfiguration.id.toByteArray())
            entityRetentionTime?.let { add(ENTITY_RETENTION_TIME, it.toByteArray()) }
        }

    private fun createTopic(resourceName: String) =
        EntityTopicNameParameters.builder()
            .orgId(formatOrgId(consumerConfiguration.orgId))
            .domainContext("fint-core")
            .resource(formatResource(resourceName))
            .build()

    fun Long.toByteArray(): ByteArray =
        ByteBuffer.allocate(Long.SIZE_BYTES).putLong(this).array()

    private fun formatResource(resourceName: String) =
        "${consumerConfiguration.domain}-${consumerConfiguration.packageName}-$resourceName"

    private fun formatOrgId(orgId: String) =
        orgId.replace(".", "-")
            .replace("_", "-")

}