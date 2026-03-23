package no.fintlabs.consumer.kafka.event

import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.health.KafkaListenerContainerHealthConfigurer
import no.fintlabs.consumer.health.KafkaListenerIds
import no.fintlabs.consumer.health.KafkaRuntimeHealthMonitor
import no.fintlabs.consumer.kafka.KafkaConsumerErrorHandling
import no.fintlabs.consumer.resource.event.EventStatusCache
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class RequestFintEventConsumer(
    private val consumerConfig: ConsumerConfiguration,
    private val eventStatusCache: EventStatusCache,
    private val kafkaRuntimeHealthMonitor: KafkaRuntimeHealthMonitor,
    private val kafkaListenerContainerHealthConfigurer: KafkaListenerContainerHealthConfigurer,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(RequestFintEventConsumer::class.java)
        private const val CONSUMER_NAME = "request-fint-event"
    }

    @Bean(name = [KafkaListenerIds.REQUEST_EVENT])
    fun requestFintEventRequestListenerContainer(
        parameterizedListenerContainerFactoryService: ParameterizedListenerContainerFactoryService,
        errorHandlerFactory: ErrorHandlerFactory,
    ): ConcurrentMessageListenerContainer<String, RequestFintEvent> {
        kafkaRuntimeHealthMonitor.registerListener(KafkaListenerIds.REQUEST_EVENT)

        return parameterizedListenerContainerFactoryService
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
                    KafkaConsumerErrorHandling.createLoggingErrorHandlerConfiguration<RequestFintEvent>(
                        logger,
                        CONSUMER_NAME,
                    ),
                ),
                kafkaListenerContainerHealthConfigurer::customize,
            ).createContainer(
                EventTopicNameParameters
                    .builder()
                    .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                            .stepBuilder()
                            .orgId(consumerConfig.orgId.asTopicSegment)
                            .domainContextApplicationDefault()
                            .build(),
                    ).eventName("${consumerConfig.domain}-${consumerConfig.packageName}-request")
                    .build(),
            ).apply { concurrency = consumerConfig.kafka.requestConcurrency }
    }

    private fun consumeRecord(consumerRecord: ConsumerRecord<String, RequestFintEvent>) {
        logger.info("Received Request: {}", consumerRecord.key())
        eventStatusCache.trackRequest(consumerRecord.value())
        kafkaRuntimeHealthMonitor.onRecordProcessed(KafkaListenerIds.REQUEST_EVENT)
    }
}
