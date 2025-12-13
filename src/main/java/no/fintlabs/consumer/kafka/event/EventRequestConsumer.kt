package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.context.ResourceContext
import no.fintlabs.consumer.resource.event.EventService
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
import org.springframework.stereotype.Component

@Component
class EventRequestConsumer(
    private val consumerConfig: ConsumerConfiguration,
    private val resourceContext: ResourceContext,
    private val eventService: EventService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Bean
    fun eventRequestListenerContainer(
        listenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String?, RequestFintEvent> =
        listenerContainerFactoryService
            .createRecordListenerContainerFactory(
                RequestFintEvent::class.java,
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
            ).createContainer(
                EntityTopicNamePatternParameters
                    .builder()
                    .topicNamePatternPrefixParameters(
                        TopicNamePatternPrefixParameters
                            .stepBuilder()
                            .orgId(TopicNamePatternParameterPattern.exactly(createOrgId()))
                            .domainContextApplicationDefault()
                            .build(),
                    ).resource(TopicNamePatternParameterPattern.anyOf(*createResourcePatterns()))
                    .build(),
            )

    private fun createResourcePatterns(): Array<String> = resourceContext.resourceNames.map { formatEventName(it) }.toTypedArray()

    private fun formatEventName(resourceName: String): String =
        "${consumerConfig.domain}-${consumerConfig.packageName}-$resourceName-request".lowercase()

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-").lowercase()

    private fun consumeRecord(consumerRecord: ConsumerRecord<String?, RequestFintEvent?>) {
        logger.debug("Received request-event: {}", consumerRecord.key())
        eventService.registerRequest(consumerRecord.key())
    }
}
