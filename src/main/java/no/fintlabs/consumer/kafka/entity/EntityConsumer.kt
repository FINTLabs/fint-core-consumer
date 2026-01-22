package no.fintlabs.consumer.kafka.entity

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceConverter
import no.fintlabs.consumer.resource.ResourceService
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Service

@Service
class EntityConsumer(
    private val resourceService: ResourceService,
    private val consumerConfig: ConsumerConfiguration,
    private val resourceConverter: ResourceConverter,
) {
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

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any>) =
        createEntityConsumerRecord(consumerRecord).let { resourceService.processEntityConsumerRecord(it) }

    private fun createEntityConsumerRecord(consumerRecord: ConsumerRecord<String, Any>): EntityConsumerRecord {
        val resourceName = getResourceName(consumerRecord.topic())
        return consumerRecord.value()?.let { recordValue ->
            resourceConverter
                .convert(resourceName, recordValue)
                .let { resource -> EntityConsumerRecord(resourceName, resource, consumerRecord) }
        } ?: EntityConsumerRecord(resourceName, null, consumerRecord)
    }

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-")

    private fun createResourcePattern() = "${consumerConfig.domain}-${consumerConfig.packageName}"

    private fun getResourceName(topic: String) = topic.substringAfterLast("-")
}
