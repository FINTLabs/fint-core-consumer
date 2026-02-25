package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
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
class RequestFintEventConsumer(
    private val configuration: ConsumerConfiguration,
    private val eventStatusCache: EventStatusCache,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Bean
    fun requestFintEventRequestListenerContainer(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
        resourceContext: ResourceContext,
    ): ConcurrentMessageListenerContainer<String, RequestFintEvent> =
        parameterizedListenerContainerFactoryService
            .createRecordListenerContainerFactory(
                RequestFintEvent::class.java,
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
                        .stepBuilder<RequestFintEvent>()
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
                            .orgId(TopicNamePatternParameterPattern.anyOf(createOrgId()))
                            .domainContextApplicationDefault()
                            .build(),
                    )
                    .eventName(
                        TopicNamePatternParameterPattern.anyOf(
                            *createEventNames(resourceContext.resourceNames),
                        ),
                    )
                    .build(),
            )

    private fun createEventNames(resourceNames: MutableSet<String>): Array<String> =
        resourceNames.map { formatEventName(it) }.toTypedArray()

    private fun formatEventName(resourceName: String?): String =
        with(configuration) {
            "$domain-$packageName-$resourceName-request"
        }

    private fun createOrgId() = configuration.orgId.replace(".", "-")

    private fun consumeRecord(consumerRecord: ConsumerRecord<String, RequestFintEvent>) {
        logger.info("Received Request: {}", consumerRecord.key())
        eventStatusCache.trackRequest(consumerRecord.value())
    }
}
