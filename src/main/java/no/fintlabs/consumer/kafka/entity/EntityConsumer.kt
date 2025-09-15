package no.fintlabs.consumer.kafka.entity

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceMapperService
import no.fintlabs.consumer.resource.ResourceService
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Service

@Service
class EntityConsumer(
    private val resourceService: ResourceService,
    private val consumerConfig: ConsumerConfiguration,
    private val resourceMapper: ResourceMapperService
) {

    @Bean
    fun entityConsumerFactory(consumerFactoryService: EntityConsumerFactoryService) =
        consumerFactoryService
            .createFactory(Any::class.java, this::consumeRecord)
            .createContainer(
                EntityTopicNamePatternParameters.builder()
                    .orgId(FormattedTopicComponentPattern.anyOf(createOrgId()))
                    .domainContext(FormattedTopicComponentPattern.anyOf("fint-core"))
                    .resource(FormattedTopicComponentPattern.startingWith(createResourcePattern()))
                    .build()
            )

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-")

    private fun createResourcePattern() =
        "${consumerConfig.domain}-${consumerConfig.packageName}"

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any>) =
        consumerRecord.takeIf { entityWasntProducedByThisConsumerInstsance(it.headers()) }
            ?.let { createKafkaEntity(consumerRecord) }
            ?.let { resourceService.handleNewEntity(it) }

    private fun createKafkaEntity(consumerRecord: ConsumerRecord<String, Any>) =
        getResourceName(consumerRecord.topic()).let { resourceName ->
            resourceMapper.mapResource(resourceName, consumerRecord.value())
                .let { resource -> KafkaEntity.from(resourceName, resource, consumerRecord) }
        }

    private fun getResourceName(topic: String) = topic.substringAfterLast("-")

    private fun entityWasntProducedByThisConsumerInstsance(headers: Headers) =
        headers.lastHeader("consumer")
            ?.value()
            ?.let { !it.contentEquals(consumerConfig.id.toByteArray()) }
            ?: true


}