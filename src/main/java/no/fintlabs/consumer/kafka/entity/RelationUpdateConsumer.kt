package no.fintlabs.consumer.kafka.entity

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.relation.RelationService
import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class RelationUpdateConsumer(
    private val relationService: RelationService,
    private val consumerConfig: ConsumerConfiguration,
) {
    @Bean
    @ConditionalOnProperty(
        name = ["fint.consumer.autorelation"],
        havingValue = "true",
        matchIfMissing = true,
    )
    open fun relationUpdateConsumerContainer(
        listenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ) = listenerContainerFactoryService
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
                    .stepBuilder<Any>()
                    .noRetries()
                    .skipFailedRecords() // TODO: We should send to DLQ - but skip temporarily
                    .build(),
            ),
        ).createContainer(
            EntityTopicNameParameters
                .builder()
                .topicNamePrefixParameters(
                    TopicNamePrefixParameters
                        .stepBuilder()
                        .orgId("fintlabs-no")
                        .domainContextApplicationDefault()
                        .build(),
                ).resourceName("relation-update")
                .build(),
        )

    fun consumeRecord(consumerRecord: ConsumerRecord<String, RelationUpdate>) =
        consumerRecord
            .value()
            .takeIf { belongsToThisService(it) }
            ?.let { relationService.processRelationUpdate(it) }

    private fun belongsToThisService(relationUpdate: RelationUpdate) =
        consumerConfig.matchesConfiguration(relationUpdate.domainName, relationUpdate.packageName, relationUpdate.orgId)
}
