package no.fintlabs.consumer.kafka.entity

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceMapperService
import no.fintlabs.consumer.resource.ResourceService
import no.fintlabs.consumer.resource.context.ResourceContext
import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Component

@Component
class EntityConsumer(
    private val resourceService: ResourceService,
    private val consumerConfig: ConsumerConfiguration,
    private val resourceMapper: ResourceMapperService,
    private val resourceContext: ResourceContext,
) {
    @Bean
    fun resourceEntityConsumerFactory(
        listenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String, Any> =
        listenerContainerFactoryService
            .createRecordListenerContainerFactory(
                Any::class.java,
                this::consumeRecord,
                ListenerConfiguration
                    .stepBuilder()
                    .groupIdApplicationDefault()
                    .maxPollRecords(1000)
                    .maxPollIntervalKafkaDefault()
                    .seekToBeginningOnAssignment()
                    .build(),
                errorHandlerFactory.createErrorHandler(
                    ErrorHandlerConfiguration
                        .stepBuilder<Any>()
                        .noRetries()
                        .skipFailedRecords() // TODO: We should send to DLQ - but skip temporarily
                        .build(),
                ),
            ).createContainer(createTopics())

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any>) {
        val resourceName = consumerRecord.getResourceName()
        val resource = consumerRecord.convertResource(resourceName)
        val kafkaEntity = createKafkaEntity(resourceName, resource, consumerRecord)
        resourceService.processEntityConsumerRecord(kafkaEntity)
    }

    private fun createTopics(): Collection<EntityTopicNameParameters> =
        resourceContext.resourceNames.map { resourceName ->
            EntityTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                    TopicNamePrefixParameters
                        .stepBuilder()
                        .orgId(createOrgId())
                        .domainContextApplicationDefault()
                        .build(),
                ).resourceName(resourceName.toTopicResource())
                .build()
        }

    private fun String.toTopicResource(): String = "${consumerConfig.domain}-${consumerConfig.packageName}-$this".lowercase()

    private fun ConsumerRecord<String, Any>.getResourceName() = topic().substringAfterLast("-")

    private fun ConsumerRecord<*, Any>.convertResource(resourceName: String) = resourceMapper.mapResource(resourceName, value())

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-")
}
