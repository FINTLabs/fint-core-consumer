package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.ResponseFintEvent
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
class EventResponseConsumer(
    private val consumerConfig: ConsumerConfiguration,
    private val resourceContext: ResourceContext,
    private val eventService: EventService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Bean
    fun eventResponseListenerContainer(
        listenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String?, ResponseFintEvent> =
        listenerContainerFactoryService
            .createRecordListenerContainerFactory(
                ResponseFintEvent::class.java,
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
                EntityTopicNamePatternParameters
                    .builder()
                    .topicNamePatternPrefixParameters(
                        TopicNamePatternPrefixParameters
                            .stepBuilder()
                            .orgId(TopicNamePatternParameterPattern.exactly(createOrgId()))
                            .domainContextApplicationDefault()
                            .build(),
                    ).resource(TopicNamePatternParameterPattern.anyOf(*createResponsePatterns()))
                    .build(),
            )

    private fun createOrgId(): String = consumerConfig.orgId.replace(".", "-").lowercase()

    private fun createResponsePatterns(): Array<String?> = resourceContext.resourceNames.map { formatEventName(it) }.toTypedArray()

    private fun formatEventName(resourceName: String?): String =
        "${consumerConfig.domain}-${consumerConfig.packageName}-$resourceName-response".lowercase()

    private fun consumeRecord(consumerRecord: ConsumerRecord<String?, ResponseFintEvent>) {
        logger.info("Received response-event: ${consumerRecord.value().corrId}")
        eventService.registerResponse(consumerRecord.value().corrId, consumerRecord.value())
    }
}
