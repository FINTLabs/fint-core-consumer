package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.RelationEventService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNamePatternParameters
import no.novari.kafka.topic.name.TopicNamePatternParameterPattern
import no.novari.kafka.topic.name.TopicNamePatternPrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class AutoRelationEntityConsumer(
    private val consumerConfig: ConsumerConfiguration,
    private val relationEventService: RelationEventService,
    private val kafkaThroughputMetrics: KafkaThroughputMetrics,
) {
    @Bean
    fun buildAutoRelationConsumer(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String, in Any> =
        parameterizedListenerContainerFactoryService
            .createRecordListenerContainerFactory(
                Any::class.java,
                this::consumeRecord,
                ListenerConfiguration
                    .stepBuilder()
                    .groupIdApplicationDefaultWithSuffix("autorelation")
                    .maxPollRecordsKafkaDefault()
                    .maxPollIntervalKafkaDefault()
                    .continueFromPreviousOffsetOnAssignment()
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
                    )
                    .resource(TopicNamePatternParameterPattern.startingWith(createResourcePattern()))
                    .build(),
            )

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any>) {
        val resourceName = consumerRecord.resourceName()
        val startedAt = System.nanoTime()
        try {
            val resource = consumerRecord.value()
            if (resource == null) {
                kafkaThroughputMetrics.recordAutoRelationEntityConsumer(resourceName, "ignored_null", System.nanoTime() - startedAt)
                return
            }
            relationEventService.addRelations(
                resourceName,
                consumerRecord.key(),
                resource,
            )
            kafkaThroughputMetrics.recordAutoRelationEntityConsumer(resourceName, "processed", System.nanoTime() - startedAt)
        } catch (ex: Exception) {
            kafkaThroughputMetrics.recordAutoRelationEntityConsumer(resourceName, "failed", System.nanoTime() - startedAt)
            throw ex
        }
    }

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-")

    private fun createResourcePattern() = "${consumerConfig.domain}-${consumerConfig.packageName}"

    private fun ConsumerRecord<String, in Any>.resourceName(): String = topic().substringAfterLast("-")
}
