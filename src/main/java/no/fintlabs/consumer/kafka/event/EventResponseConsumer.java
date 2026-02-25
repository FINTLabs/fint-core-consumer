package no.fintlabs.consumer.kafka.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.ResourceContext;
import no.fintlabs.consumer.resource.event.EventStatusCache;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ErrorHandlerFactory;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService;
import no.novari.kafka.topic.name.EventTopicNamePatternParameters;
import no.novari.kafka.topic.name.TopicNamePatternParameterPattern;
import no.novari.kafka.topic.name.TopicNamePatternPrefixParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Set;
import java.util.stream.Stream;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class EventResponseConsumer {

    private final ConsumerConfiguration configuration;
    private final EventStatusCache eventStatusCache;

    @Bean
    public ConcurrentMessageListenerContainer<String, ResponseFintEvent> someOtherBeanNameTired(
            ParameterizedListenerContainerFactoryService parameterizedListenerContainerFactoryService,
            ErrorHandlerFactory errorHandlerFactory,
            ResourceContext resourceContext) {
        return parameterizedListenerContainerFactoryService
                .createRecordListenerContainerFactory(
                        ResponseFintEvent.class,
                        this::consumeRecord,
                        ListenerConfiguration
                                .stepBuilder()
                                .groupIdApplicationDefault()
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .continueFromPreviousOffsetOnAssignment()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<ResponseFintEvent>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        )
                )
                .createContainer(
                        EventTopicNamePatternParameters.builder()
                                .topicNamePatternPrefixParameters(
                                        TopicNamePatternPrefixParameters
                                                .stepBuilder()
                                                .orgId(TopicNamePatternParameterPattern.anyOf(createOrgId()))
                                                .domainContextApplicationDefault()
                                                .build()
                                )
                                .eventName(TopicNamePatternParameterPattern.anyOf(
                                        createEventNames(resourceContext.getResourceNames())
                                ))
                                .build()
                );
    }

    private String[] createEventNames(Set<String> resourceNames) {
        return resourceNames.stream()
                .flatMap(resourceName -> Stream.of(formatEventName(resourceName)))
                .toArray(String[]::new);
    }

    private String formatEventName(String resourceName) {
        return "%s-%s-%s-response".formatted(
                configuration.getDomain(),
                configuration.getPackageName(),
                resourceName
        );
    }

    private String createOrgId() {
        return configuration.getOrgId().replace(".", "-");
    }

    private void consumeRecord(ConsumerRecord<String, ResponseFintEvent> consumerRecord) {
        log.info("Received Response: {}", consumerRecord.value());
        eventStatusCache.trackResponse(consumerRecord.value().getCorrId(), consumerRecord.value());
    }
}
