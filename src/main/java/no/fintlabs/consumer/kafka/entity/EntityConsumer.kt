package no.fintlabs.consumer.kafka.entity

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.kafka.KafkaConsumerErrorHandling
import no.fintlabs.consumer.kafka.stringValue
import no.fintlabs.consumer.resource.ResourceConverter
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Service

@Service
class EntityConsumer(
    private val entityProcessingService: EntityProcessingService,
    private val consumerConfig: ConsumerConfiguration,
    private val resourceConverter: ResourceConverter,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(EntityConsumer::class.java)
        private const val CONSUMER_NAME = "entity"
    }

    @Bean
    fun resourceEntityConsumerFactory(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String, in Any> =
        parameterizedListenerContainerFactoryService
            .createRecordListenerContainerFactory(
                Any::class.java,
                this::consumeRecord,
                ListenerConfiguration
                    .stepBuilder()
                    .groupIdApplicationDefaultWithUniqueSuffix()
                    .maxPollRecordsKafkaDefault()
                    .maxPollIntervalKafkaDefault()
                    .seekToBeginningOnAssignment()
                    .build(),
                errorHandlerFactory.createErrorHandler(
                    KafkaConsumerErrorHandling.createLoggingErrorHandlerConfiguration<Any>(
                        logger,
                        CONSUMER_NAME,
                    ),
                ),
            ).createContainer(
                EntityTopicNameParameters
                    .builder()
                    .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                            .stepBuilder()
                            .orgId(consumerConfig.orgId.asTopicSegment)
                            .domainContextApplicationDefault()
                            .build(),
                    ).resourceName("${consumerConfig.domain}-${consumerConfig.packageName}")
                    .build(),
            ).apply { concurrency = consumerConfig.kafka.entityConcurrency }

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any?>) =
        createEntityConsumerRecord(consumerRecord)
            .let { entityProcessingService.processEntityConsumerRecord(it) }

    private fun createEntityConsumerRecord(consumerRecord: ConsumerRecord<String, Any?>) =
        consumerRecord.getResourceName().let { resourceName ->
            consumerRecord
                .value()
                ?.let { resourceConverter.convert(resourceName, it) }
                ?.let { EntityConsumerRecord(resourceName, it, consumerRecord) }
                ?: EntityConsumerRecord(resourceName, null, consumerRecord)
        }

    private fun ConsumerRecord<String, Any?>.getResourceName(): String =
        headers().stringValue(RESOURCE_NAME) ?: throw IllegalArgumentException("Resource name header not found")
}
