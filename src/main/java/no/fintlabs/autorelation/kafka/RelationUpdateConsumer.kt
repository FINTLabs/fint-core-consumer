package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.health.InitialKafkaBootstrapTracker
import no.fintlabs.consumer.health.KafkaListenerContainerHealthConfigurer
import no.fintlabs.consumer.health.KafkaListenerIds
import no.fintlabs.consumer.health.KafkaRuntimeHealthMonitor
import no.fintlabs.consumer.kafka.KafkaConsumerErrorHandling
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNamePatternParameters
import no.novari.kafka.topic.name.TopicNamePatternParameterPattern
import no.novari.kafka.topic.name.TopicNamePatternPrefixParameters
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
    private val initialKafkaBootstrapTracker: InitialKafkaBootstrapTracker,
    private val kafkaRuntimeHealthMonitor: KafkaRuntimeHealthMonitor,
    private val kafkaListenerContainerHealthConfigurer: KafkaListenerContainerHealthConfigurer,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(RelationUpdateConsumer::class.java)
        private const val CONSUMER_NAME = "relation-update"
    }

    @Bean(name = [KafkaListenerIds.RELATION_UPDATE])
    @ConditionalOnProperty(
        name = ["fint.consumer.autorelation"],
        havingValue = "true",
        matchIfMissing = true,
    )
    fun relationUpdateConsumerContainer(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String, RelationUpdate> {
        initialKafkaBootstrapTracker.registerBlockingListener(KafkaListenerIds.RELATION_UPDATE)
        kafkaRuntimeHealthMonitor.registerListener(KafkaListenerIds.RELATION_UPDATE)

        return parameterizedListenerContainerFactoryService
            .createRecordListenerContainerFactory(
                RelationUpdate::class.java,
                this::consumeRecord,
                ListenerConfiguration
                    .stepBuilder()
                    .groupIdApplicationDefault()
                    .maxPollRecordsKafkaDefault()
                    .maxPollIntervalKafkaDefault()
                    .seekToBeginningAndPerformOperationOnAssignment { assignments ->
                        initialKafkaBootstrapTracker.onPartitionsAssigned(
                            KafkaListenerIds.RELATION_UPDATE,
                            assignments.keys,
                        )
                    }.onRevocation { partitions ->
                        initialKafkaBootstrapTracker.onPartitionsRevoked(KafkaListenerIds.RELATION_UPDATE, partitions)
                    }.build(),
                errorHandlerFactory.createErrorHandler(
                    KafkaConsumerErrorHandling.createLoggingErrorHandlerConfiguration<RelationUpdate>(
                        logger,
                        CONSUMER_NAME,
                    ),
                ),
                kafkaListenerContainerHealthConfigurer::customize,
            ).createContainer(
                EntityTopicNamePatternParameters
                    .builder()
                    .topicNamePatternPrefixParameters(
                        TopicNamePatternPrefixParameters
                            .stepBuilder()
                            .orgId(TopicNamePatternParameterPattern.exactly(consumerConfig.orgId.asTopicSegment))
                            .domainContextApplicationDefault()
                            .build(),
                        // Makes sure we listen to component patterns such as utdanning-vurdering'-relation-update'
                    ).resource(TopicNamePatternParameterPattern.endingWith("-relation-update"))
                    .build(),
            ).apply { concurrency = consumerConfig.kafka.relationConcurrency }
    }

    fun consumeRecord(consumerRecord: ConsumerRecord<String?, RelationUpdate>) {
        val startedAt = System.nanoTime()
        val relationUpdate = consumerRecord.value()

        if (relationUpdate == null) {
            kafkaThroughputMetrics.recordRelationUpdateConsumer(null, "ignored_null", System.nanoTime() - startedAt)
            initialKafkaBootstrapTracker.onRecordProcessed(KafkaListenerIds.RELATION_UPDATE, consumerRecord)
            kafkaRuntimeHealthMonitor.onRecordProcessed(KafkaListenerIds.RELATION_UPDATE)
            return
        }

        if (!relationUpdate.belongsToThisService()) {
            kafkaThroughputMetrics.recordRelationUpdateConsumer(
                relationUpdate.targetEntity.resourceName,
                "ignored_foreign_component",
                System.nanoTime() - startedAt,
            )
            initialKafkaBootstrapTracker.onRecordProcessed(KafkaListenerIds.RELATION_UPDATE, consumerRecord)
            kafkaRuntimeHealthMonitor.onRecordProcessed(KafkaListenerIds.RELATION_UPDATE)
            return
        }

        try {
            autoRelationService.applyOrBufferUpdate(relationUpdate)
            kafkaThroughputMetrics.recordRelationUpdateConsumer(
                relationUpdate.targetEntity.resourceName,
                "processed",
                System.nanoTime() - startedAt,
            )
            initialKafkaBootstrapTracker.onRecordProcessed(KafkaListenerIds.RELATION_UPDATE, consumerRecord)
            kafkaRuntimeHealthMonitor.onRecordProcessed(KafkaListenerIds.RELATION_UPDATE)
        } catch (ex: Exception) {
            kafkaThroughputMetrics.recordRelationUpdateConsumer(
                relationUpdate.targetEntity.resourceName,
                "failed",
                System.nanoTime() - startedAt,
            )
            kafkaRuntimeHealthMonitor.onRecordProcessed(KafkaListenerIds.RELATION_UPDATE)
            throw ex
        }
    }

    private fun RelationUpdate.belongsToThisService() =
        with(targetEntity) {
            consumerConfig.matchesComponent(domainName, packageName)
        }
}
