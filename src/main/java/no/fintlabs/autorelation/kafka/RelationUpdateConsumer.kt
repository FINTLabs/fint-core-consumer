package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class RelationUpdateConsumer(
    private val autoRelationService: AutoRelationService,
    private val consumerConfig: ConsumerConfiguration,
) {
    @Bean
    @ConditionalOnProperty(
        name = ["fint.consumer.autorelation"],
        havingValue = "true",
        matchIfMissing = true,
    )
    fun relationUpdateConsumerContainer(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String?, RelationUpdate> {
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
                    ErrorHandlerConfiguration
                        .stepBuilder<RelationUpdate>()
                        .noRetries()
                        .skipFailedRecords()
                        .build(),
                ),
            ).createContainer(
                EventTopicNameParameters
                    .builder()
                    .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                            .stepBuilder()
                            .orgId(consumerConfig.orgId.toTopicFormat())
                            .domainContextApplicationDefault()
                            .build(),
                    )
                    .eventName("relation-update")
                    .build(),
            )
    }

    fun consumeRecord(consumerRecord: ConsumerRecord<String?, RelationUpdate>) =
        consumerRecord
            .value()
            .takeIf { it.belongsToThisService() }
            ?.run { autoRelationService.applyOrBufferUpdate(this) }

    private fun RelationUpdate.belongsToThisService() =
        with(targetEntity) {
            consumerConfig.matchesComponent(domainName, packageName)
        }

    private fun String.toTopicFormat() = replace(".", "-")
}
