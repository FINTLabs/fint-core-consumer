package no.fintlabs.consumer.kafka.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.ResourceContext;
import no.fintlabs.consumer.resource.event.EventService;
import no.fintlabs.kafka.common.topic.pattern.ValidatedTopicComponentPattern;
import no.fintlabs.kafka.event.EventConsumerFactoryService;
import no.fintlabs.kafka.event.topic.EventTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Set;
import java.util.stream.Stream;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class EventRequestConsumer {

    private final ConsumerConfiguration configuration;
    private final EventService eventService;

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
                .flatMap(rn -> Stream.of(formatEventName(rn)))
                .toArray(String[]::new);
    }

    private String formatEventName(String resourceName) {
        return "%s-%s-%s-request".formatted(
                configuration.getDomain(),
                configuration.getPackageName(),
                resourceName
        );
    }

    private void consumeRecord(ConsumerRecord<String, RequestFintEvent> consumerRecord) {
        log.info("Received Request: {}", consumerRecord.key());
        eventService.registerRequest(consumerRecord.key());
    }
}
