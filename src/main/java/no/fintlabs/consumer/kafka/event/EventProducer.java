package no.fintlabs.consumer.kafka.event;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.adapter.models.OperationType;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.kafka.event.EventProducerFactory;
import no.fintlabs.kafka.event.EventProducerRecord;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import no.fintlabs.kafka.event.topic.EventTopicService;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Service
@Slf4j
public class EventProducer {

    private static final int RETENTION_TIME_MS = 172800000;
    private final no.fintlabs.kafka.event.EventProducer<String> eventProducer;
    private final EventTopicService eventTopicService;
    private final ConsumerConfiguration configuration;
    private final Set<String> topics = new HashSet<>();

    public EventProducer(EventProducerFactory eventProducerFactory, EventTopicService eventTopicService, ConsumerConfiguration configuration) {
        eventProducer = eventProducerFactory.createProducer(String.class);
        this.configuration = configuration;
        this.eventTopicService = eventTopicService;
    }

    public void sendEvent(String resourceName, String resourceData, OperationType operationType) {
        RequestFintEvent requestFintEvent = createRequestFintEvent(resourceName, resourceData, operationType);
        String eventName = createEventName(requestFintEvent);
        EventTopicNameParameters eventTopicNameParameters = EventTopicNameParameters.builder().eventName(eventName).build();

        ensureTopicIfItDoesntExist(eventName, eventTopicNameParameters);
        log.info("Sending event-id: {} - {}", requestFintEvent.getCorrId(), eventName);
        eventProducer.send(createProducerRecord(requestFintEvent.getCorrId(), eventTopicNameParameters, requestFintEvent.getValue()));
    }

    private EventProducerRecord<String> createProducerRecord(String key, EventTopicNameParameters eventTopicNameParameters, String resourceData) {
        return EventProducerRecord.<String>builder()
                .key(key)
                .topicNameParameters(eventTopicNameParameters)
                .value(resourceData)
                .build();
    }

    private void ensureTopicIfItDoesntExist(String eventName, EventTopicNameParameters eventTopicNameParameters) {
        if (!topics.contains(eventName)) {
            log.info("Ensuring event topic: {}", eventName);
            eventTopicService.ensureTopic(eventTopicNameParameters, RETENTION_TIME_MS);
            topics.add(eventName);
        }
    }

    private RequestFintEvent createRequestFintEvent(String resourceName, String resourceData, OperationType operationType) {
        return RequestFintEvent.builder()
                .corrId(UUID.randomUUID().toString())
                .domainName(configuration.getDomain())
                .packageName(configuration.getPackageName())
                .orgId(configuration.getOrgId())
                .created(System.currentTimeMillis())
                .resourceName(resourceName)
                .value(resourceData)
                .operationType(operationType)
                .build();
    }

    private String createEventName(RequestFintEvent requestFintEvent) {
        return "%s-%s-%s-%s-request".formatted(
                configuration.getDomain(),
                configuration.getPackageName(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getOperationType().toString().toLowerCase()
        );
    }

}
