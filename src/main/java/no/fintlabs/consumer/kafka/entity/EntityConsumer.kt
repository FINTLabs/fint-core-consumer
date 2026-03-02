package no.fintlabs.consumer.kafka.entity

import java.time.Duration
import no.fintlabs.consumer.config.ConsumerConfiguration
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
        val startNanos = System.nanoTime()
        var failed = false
        var corrId: String? = null

        try {
            val record = createEntityConsumerRecord(consumerRecord)
            corrId = record.corrId
            entityProcessingService.processEntityConsumerRecord(record)
        } catch (exception: Exception) {
            failed = true
            throw exception
        } finally {
            val duration = Duration.ofNanos(System.nanoTime() - startNanos)
            if (duration > SLOW_PROCESSING_THRESHOLD) {
                logger.warn(
                    "Slow Kafka message processing: durationMs={}, topic={}, partition={}, offset={}, key={}, corrId={}, failed={}",
                    duration.toMillis(),
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.key(),
                    corrId,
                    failed,
                )
            }
        }
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

    companion object {
        private val logger = LoggerFactory.getLogger(EntityConsumer::class.java)
        private val SLOW_PROCESSING_THRESHOLD = Duration.ofSeconds(10)
    }
}
