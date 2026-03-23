package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.RelationEventService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.health.KafkaListenerContainerHealthConfigurer
import no.fintlabs.consumer.health.KafkaListenerIds
import no.fintlabs.consumer.health.KafkaRuntimeHealthMonitor
import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.kafka.KafkaConsumerErrorHandling
import no.fintlabs.consumer.kafka.entity.extractIdentifier
import no.fintlabs.consumer.kafka.stringValue
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class AutoRelationEntityConsumer(
    private val consumerConfig: ConsumerConfiguration,
    private val relationEventService: RelationEventService,
    private val kafkaRuntimeHealthMonitor: KafkaRuntimeHealthMonitor,
    private val kafkaListenerContainerHealthConfigurer: KafkaListenerContainerHealthConfigurer,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(AutoRelationEntityConsumer::class.java)
        private const val CONSUMER_NAME = "autorelation-entity"
    }

    @Bean(name = [KafkaListenerIds.AUTORELATION_ENTITY])
    fun buildAutoRelationConsumer(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String, in Any> {
        kafkaRuntimeHealthMonitor.registerListener(KafkaListenerIds.AUTORELATION_ENTITY)

        return parameterizedListenerContainerFactoryService
            .createRecordListenerContainerFactory(
                Any::class.java,
                this::consumeRecord,
                ListenerConfiguration
                    .stepBuilder()
                    .groupIdApplicationDefaultWithSuffix("autorelation")
                    .maxPollRecordsKafkaDefault()
                    .maxPollIntervalKafkaDefault()
                    .let { step ->
                        if (consumerConfig.kafka.relationEntitySeekToBeginning) {
                            step.seekToBeginningOnAssignment()
                        } else {
                            step.continueFromPreviousOffsetOnAssignment()
                        }
                    }.build(),
                errorHandlerFactory.createErrorHandler(
                    KafkaConsumerErrorHandling.createLoggingErrorHandlerConfiguration<Any>(
                        logger,
                        CONSUMER_NAME,
                    ),
                ),
                kafkaListenerContainerHealthConfigurer::customize,
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
    }

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any?>) {
        consumerRecord
            .value()
            ?.let { resource ->
                relationEventService.addRelations(
                    consumerRecord.getResourceName(),
                    consumerRecord.extractIdentifier(),
                    resource,
                )
            }
        kafkaRuntimeHealthMonitor.onRecordProcessed(KafkaListenerIds.AUTORELATION_ENTITY)
    }

    private fun ConsumerRecord<String, Any?>.getResourceName(): String =
        headers().stringValue(RESOURCE_NAME) ?: throw IllegalArgumentException("Resource name header not found")
}
