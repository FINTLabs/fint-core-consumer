package no.fintlabs.consumer.kafka

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.entity.EntityProducerFactory
import no.fintlabs.kafka.entity.EntityProducerRecord
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import no.fintlabs.kafka.entity.topic.EntityTopicService
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class KafkaUtils(
    entityProducerFactory: EntityProducerFactory,
    private val kafkaAdmin: KafkaAdmin,
    private val entityTopicService: EntityTopicService,
    private val consumerConfiguration: ConsumerConfiguration,
) {

    private val retentionTime = Duration.ofDays(7).toMillis()
    private val entityProducer = entityProducerFactory.createProducer(Any::class.java)

    fun ensureTopic(vararg resources: String) =
        resources.forEach { resource ->
            entityTopicService.ensureTopic(createEntityTopic(resource), retentionTime)
        }

    fun purgeTopics(vararg resources: String) {
        val topicNames = resources.map { createResourceTopic(it) }
        AdminClient.create(kafkaAdmin.configurationProperties).use { admin ->
            val desc = admin.describeTopics(topicNames.toList()).allTopicNames().get()
            val tps = desc.flatMap { (topic, td) ->
                td.partitions().map { TopicPartition(topic, it.partition()) }
            }

            val endOffsets = admin.listOffsets(
                tps.associateWith { OffsetSpec.latest() }
            ).all().get()

            val toDelete = endOffsets.mapValues { (_, v) -> RecordsToDelete.beforeOffset(v.offset()) }
            admin.deleteRecords(toDelete).all().get()
        }
    }

    fun produceEntity(resourceId: String, resource: String, resourceObject: Any) =
        entityProducer.send(
            EntityProducerRecord.builder<Any>()
                .key(resourceId)
                .topicNameParameters(createEntityTopic(resource))
                .value(resourceObject)
                .build()
        )

    private fun createEntityTopic(resource: String) =
        EntityTopicNameParameters.builder()
            .orgId(formatOrg(consumerConfiguration.orgId))
            .domainContext("fint-core")
            .resource(createResource(resource))
            .build()

    private fun formatOrg(org: String) = org.replace(".", "-")

    private fun createResource(resource: String) =
        "${consumerConfiguration.domain}-${consumerConfiguration.packageName}-$resource".lowercase()

    private fun createResourceTopic(resource: String) =
        "${formatOrg(consumerConfiguration.orgId)}.fint-core.entity.${consumerConfiguration.domain}-${consumerConfiguration.packageName}-$resource".lowercase()

}