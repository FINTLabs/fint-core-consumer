package no.fintlabs.consumer.kafka.entity

import mu.KotlinLogging
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceConverter
import no.fintlabs.consumer.resource.ResourceService
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters
import no.novari.fint.model.resource.FintResource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Service

@Service
class EntityConsumer(
    private val resourceService: ResourceService,
    private val consumerConfig: ConsumerConfiguration,
    private val resourceConverter: ResourceConverter,
) {
    private val logger = KotlinLogging.logger { }

    @Bean
    fun resourceEntityConsumerFactory(consumerFactoryService: EntityConsumerFactoryService) =
        consumerFactoryService
            .createFactory(Any::class.java, this::consumeRecord)
            .createContainer(
                EntityTopicNamePatternParameters
                    .builder()
                    .orgId(FormattedTopicComponentPattern.anyOf(createOrgId()))
                    .domainContext(FormattedTopicComponentPattern.anyOf("fint-core"))
                    .resource(FormattedTopicComponentPattern.startingWith(createResourcePattern()))
                    .build(),
            ) // TODO: Upgrade to fint-kafka 5 - skip failed messages & commit them onto a DLQ

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any?>) {
        logger.info { "Processing record: $consumerRecord" }
        createEntityConsumerRecord(consumerRecord)
            .let { resourceService.processEntityConsumerRecord(it) }
    }

    private fun createEntityConsumerRecord(consumerRecord: ConsumerRecord<String, Any?>) =
        getResourceName(consumerRecord.topic()).let { resourceName ->

            consumerRecord
                .value()
                ?.let { resourceConverter.convert(resourceName, it) }
                ?.let { createKafkaEntity(resourceName, it, consumerRecord) }
                ?: createKafkaEntity(resourceName, null, consumerRecord)
        }

    private fun createKafkaEntity(
        resourceName: String,
        resource: FintResource?,
        consumerRecord: ConsumerRecord<String, Any?>,
    ) = EntityConsumerRecord(resourceName, resource, record = consumerRecord)

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-")

    private fun createResourcePattern() = "${consumerConfig.domain}-${consumerConfig.packageName}"

    private fun getResourceName(topic: String) = topic.substringAfterLast("-")
}
