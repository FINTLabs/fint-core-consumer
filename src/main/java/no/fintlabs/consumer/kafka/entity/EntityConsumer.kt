package no.fintlabs.consumer.kafka.entity

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.fintlabs.consumer.resource.ResourceConverter
import no.novari.fint.model.resource.FintResource
import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNamePatternParameters
import no.novari.kafka.topic.name.TopicNamePatternParameterPattern
import no.novari.kafka.topic.name.TopicNamePatternPrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Service

@Service
class EntityConsumer(
    private val entityProcessingService: EntityProcessingService,
    private val consumerConfig: ConsumerConfiguration,
    private val resourceConverter: ResourceConverter,
    private val kafkaThroughputMetrics: KafkaThroughputMetrics,
) {
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
                    .groupIdApplicationDefault()
                    .maxPollRecordsKafkaDefault()
                    .maxPollIntervalKafkaDefault()
                    .seekToBeginningOnAssignment()
                    .build(),
                errorHandlerFactory.createErrorHandler(
                    ErrorHandlerConfiguration
                        .stepBuilder<Any>()
                        .noRetries()
                        .skipFailedRecords()
                        .build(),
                ),
            ).createContainer(
                EntityTopicNamePatternParameters
                    .builder()
                    .topicNamePatternPrefixParameters(
                        TopicNamePatternPrefixParameters
                            .stepBuilder()
                            .orgId(TopicNamePatternParameterPattern.anyOf(createOrgId()))
                            .domainContextApplicationDefault()
                            .build(),
                    ).resource(TopicNamePatternParameterPattern.startingWith(createResourcePattern()))
                    .build(),
            )

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any?>) {
        val resourceName = getResourceName(consumerRecord.topic())
        val startedAt = System.nanoTime()
        try {
            createEntityConsumerRecord(resourceName, consumerRecord).let { entityProcessingService.processEntityConsumerRecord(it) }
            kafkaThroughputMetrics.recordEntityConsumer(resourceName, "processed", System.nanoTime() - startedAt)
        } catch (ex: Exception) {
            kafkaThroughputMetrics.recordEntityConsumer(resourceName, "failed", System.nanoTime() - startedAt)
            throw ex
        }
    }

    private fun createEntityConsumerRecord(
        resourceName: String,
        consumerRecord: ConsumerRecord<String, Any?>,
    ) = consumerRecord
        .value()
        ?.let { resourceConverter.convert(resourceName, it) }
        ?.let { createKafkaEntity(resourceName, it, consumerRecord) }
        ?: createKafkaEntity(resourceName, null, consumerRecord)

    private fun createKafkaEntity(
        resourceName: String,
        resource: FintResource?,
        consumerRecord: ConsumerRecord<String, Any?>,
    ) = EntityConsumerRecord(resourceName, resource, record = consumerRecord)

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-")

    private fun createResourcePattern() = "${consumerConfig.domain}-${consumerConfig.packageName}"

    private fun getResourceName(topic: String) = topic.substringAfterLast("-")
}
