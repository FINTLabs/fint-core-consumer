package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaConsumerErrorHandling
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class RelationUpdateConsumer(
    private val autoRelationService: AutoRelationService,
    private val consumerConfig: ConsumerConfiguration,
    private val kafkaThroughputMetrics: KafkaThroughputMetrics,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(RelationUpdateConsumer::class.java)
        private const val CONSUMER_NAME = "relation-update"
    }

    @Bean
    @ConditionalOnProperty(
        name = ["fint.consumer.autorelation"],
        havingValue = "true",
        matchIfMissing = true,
    )
    fun relationUpdateConsumerContainer(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String, RelationUpdate> {
        return parameterizedListenerContainerFactoryService
            .createRecordListenerContainerFactory(
                RelationUpdate::class.java,
                this::consumeRecord,
                ListenerConfiguration
                    .stepBuilder()
                    .groupIdApplicationDefault()
                    .maxPollRecordsKafkaDefault()
                    .maxPollIntervalKafkaDefault()
                    .seekToBeginningOnAssignment()
                    .build(),
                errorHandlerFactory.createErrorHandler(
                    KafkaConsumerErrorHandling.createLoggingErrorHandlerConfiguration<RelationUpdate>(
                        logger,
                        CONSUMER_NAME,
                    ),
                ),
            ).createContainer(
                EventTopicNameParameters
                    .builder()
                    .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                            .stepBuilder()
                            .orgId(consumerConfig.orgId.asTopicSegment)
                            .domainContextApplicationDefault()
                            .build(),
                    ).eventName("relation-update")
                    .build(),
            ).apply { consumerConfig.kafka.relationConcurrency }
    }

    fun consumeRecord(consumerRecord: ConsumerRecord<String?, RelationUpdate>) {
        val startedAt = System.nanoTime()
        val relationUpdate = consumerRecord.value()

        if (relationUpdate == null) {
            kafkaThroughputMetrics.recordRelationUpdateConsumer(null, "ignored_null", System.nanoTime() - startedAt)
            return
        }

        if (!relationUpdate.belongsToThisService()) {
            kafkaThroughputMetrics.recordRelationUpdateConsumer(
                relationUpdate.targetEntity.resourceName,
                "ignored_foreign_component",
                System.nanoTime() - startedAt,
            )
            return
        }

        try {
            autoRelationService.applyOrBufferUpdate(relationUpdate)
            kafkaThroughputMetrics.recordRelationUpdateConsumer(
                relationUpdate.targetEntity.resourceName,
                "processed",
                System.nanoTime() - startedAt,
            )
        } catch (ex: Exception) {
            kafkaThroughputMetrics.recordRelationUpdateConsumer(
                relationUpdate.targetEntity.resourceName,
                "failed",
                System.nanoTime() - startedAt,
            )
            throw ex
        }
    }

    private fun RelationUpdate.belongsToThisService() =
        with(targetEntity) {
            consumerConfig.matchesComponent(domainName, packageName)
        }
}
