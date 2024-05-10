package no.fintlabs.consumer.kafka.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.adapter.models.OperationType;
import no.fintlabs.adapter.models.ResponseFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.EventStatusService;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;
import no.fintlabs.kafka.event.EventConsumerFactoryService;
import no.fintlabs.kafka.event.topic.EventTopicNamePatternParameters;
import no.fintlabs.reflection.ReflectionService;
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
public class EventResponseConsumer {

    private final ConsumerConfiguration configuration;
    private final EventStatusService eventStatusService;

    // TODO: ResponseFintEvent shouldnt be of T, it should contain data as String

    @Bean
    public ConcurrentMessageListenerContainer<String, ResponseFintEvent> someOtherBeanNameTired(
            EventConsumerFactoryService eventConsumerFactoryService,
            ReflectionService reflectionService) {
        return eventConsumerFactoryService
                .createFactory(ResponseFintEvent.class, this::consumeRecord)
                .createContainer(
                        EventTopicNamePatternParameters.builder()
                                .eventName(ValidatedTopicComponentPattern.anyOf(
                                        createEventNames(reflectionService.getResources().keySet())
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
        return "%s-%s-%s-%s-response".formatted(
                configuration.getDomain(),
                configuration.getPackageName(),
                resourceName,
                operationType.toString().toLowerCase()
        );
    }

    private void consumeRecord(ConsumerRecord<String, ResponseFintEvent> consumerRecord) {
        log.info("Received Response: {}", consumerRecord.key());
        eventStatusService.registerResponse(consumerRecord.key(), consumerRecord.value());
    }
}
