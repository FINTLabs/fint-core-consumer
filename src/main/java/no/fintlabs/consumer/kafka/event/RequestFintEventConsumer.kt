package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.context.ResourceContext
import no.fintlabs.consumer.resource.event.EventStatusCache
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern
import no.fintlabs.kafka.event.EventConsumerConfiguration
import no.fintlabs.kafka.event.EventConsumerFactoryService
import no.fintlabs.kafka.event.topic.EventTopicNamePatternParameters
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
        eventConsumerFactoryService: EventConsumerFactoryService,
        resourceContext: ResourceContext,
    ): ConcurrentMessageListenerContainer<String, RequestFintEvent> =
        eventConsumerFactoryService
            .createFactory(
                RequestFintEvent::class.java,
                this::consumeRecord,
                EventConsumerConfiguration
                    .builder()
                    .seekingOffsetResetOnAssignment(true)
                    .build(),
            ).createContainer(
                EventTopicNamePatternParameters
                    .builder()
                    .eventName(ValidatedTopicComponentPattern.anyOf(*createEventNames(resourceContext.resourceNames)))
                    .build(),
            )

    private fun createEventNames(resourceNames: MutableSet<String>): Array<String> =
        resourceNames.map { formatEventName(it) }.toTypedArray()

    private fun formatEventName(resourceName: String?): String =
        with(configuration) {
            "$domain-$packageName-$resourceName-request"
        }

    private fun consumeRecord(consumerRecord: ConsumerRecord<String, RequestFintEvent>) {
        logger.info("Received Request: {}", consumerRecord.key())
        eventStatusCache.trackRequest(consumerRecord.value())
    }
}
