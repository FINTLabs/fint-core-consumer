package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.ResponseFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.context.ResourceContext
import no.fintlabs.consumer.resource.event.EventStatusCache
import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EventTopicNamePatternParameters
import no.novari.kafka.topic.name.TopicNamePatternParameterPattern
import no.novari.kafka.topic.name.TopicNamePatternPrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class EventResponseConsumer(
    private val configuration: ConsumerConfiguration,
    private val eventStatusCache: EventStatusCache,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Bean
    fun responseFintEventContainerListener(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
        resourceContext: ResourceContext,
    ): ConcurrentMessageListenerContainer<String, ResponseFintEvent> =
        parameterizedListenerContainerFactoryService
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
                        .stepBuilder<ResponseFintEvent>()
                        .noRetries()
                        .skipFailedRecords()
                        .build(),
                ),
            ).createContainer(
                EventTopicNamePatternParameters
                    .builder()
                    .topicNamePatternPrefixParameters(
                        TopicNamePatternPrefixParameters
                            .stepBuilder()
                            .orgId(TopicNamePatternParameterPattern.anyOf(configuration.orgId.asTopicSegment))
                            .domainContextApplicationDefault()
                            .build(),
                    ).eventName(
                        TopicNamePatternParameterPattern.anyOf(
                            *createEventNames(resourceContext.resourceNames),
                        ),
                    ).build(),
            )

    private fun createEventNames(resourceNames: Set<String>): Array<String> =
        resourceNames.map(::formatEventName).toTypedArray()

    private fun formatEventName(resourceName: String): String =
        with(configuration) {
            "$domain-$packageName-$resourceName-response"
        }

    private fun consumeRecord(consumerRecord: ConsumerRecord<String, ResponseFintEvent>) {
        logger.info("Received Response: {}", consumerRecord.value())
        eventStatusCache.trackResponse(consumerRecord.value().corrId, consumerRecord.value())
    }
}
