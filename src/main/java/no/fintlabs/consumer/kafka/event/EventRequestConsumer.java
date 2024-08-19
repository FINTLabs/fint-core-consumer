package no.fintlabs.consumer.kafka.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.adapter.models.OperationType;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;
import no.fintlabs.kafka.event.EventConsumerFactoryService;
import no.fintlabs.kafka.event.topic.EventTopicNamePatternParameters;
import no.fintlabs.reflection.ResourceContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Set;
import java.util.stream.Stream;

@Slf4j
@Configuration
@RequiredArgsConstructor
@ComponentScan("no.fintlabs.kafka")
public class EventRequestConsumer {

    private final ConsumerConfiguration configuration;
    private final EventService eventService;

    // TODO: Consider topic names for requests
    // Do we really need to have the operationType & Resource in the topic name when that data is kept within the RequestEvent object?

    @Bean
    public ConcurrentMessageListenerContainer<String, RequestFintEvent> someBeanNameImSoTired(
            EventConsumerFactoryService eventConsumerFactoryService,
            ResourceContext resourceContext) {
        return eventConsumerFactoryService
                .createFactory(RequestFintEvent.class, this::consumeRecord)
                .createContainer(
                        EventTopicNamePatternParameters.builder()
                                .eventName(ValidatedTopicComponentPattern.anyOf(
                                        createEventNames(resourceContext.getResourceNames())
                                ))
                                .build()
                );
    }

    private String[] createEventNames(Set<String> resourceNames) {
        return resourceNames.stream()
                .flatMap(this::generateEventNamesForKey)
                .toArray(String[]::new);
    }

    private Stream<String> generateEventNamesForKey(String resourceName) {
        return Stream.of(
                formatEventName(resourceName, OperationType.CREATE),
                formatEventName(resourceName, OperationType.UPDATE)
        );
    }

    private String formatEventName(String resourceName, OperationType operationType) {
        return "%s-%s-%s-%s-request".formatted(
                configuration.getDomain(),
                configuration.getPackageName(),
                resourceName,
                operationType.toString().toLowerCase()
        );
    }

    private void consumeRecord(ConsumerRecord<String, RequestFintEvent> consumerRecord) {
        log.info("Received Request: {}", consumerRecord.key());
        eventService.registerRequest(consumerRecord.key(), consumerRecord.value());
    }
}
