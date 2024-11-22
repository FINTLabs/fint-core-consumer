package no.fintlabs.consumer.kafka.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.operation.OperationType;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.ResourceService;
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
    private final no.fintlabs.kafka.event.EventProducer<RequestFintEvent> eventProducer;
    private final EventTopicService eventTopicService;
    private final ConsumerConfiguration configuration;
    private final ResourceService resourceService;
    private final ObjectMapper objectMapper;
    private final Set<String> topics = new HashSet<>();

    public EventProducer(EventProducerFactory eventProducerFactory, EventTopicService eventTopicService, ConsumerConfiguration configuration, ResourceService resourceService, ObjectMapper objectMapper) {
        eventProducer = eventProducerFactory.createProducer(RequestFintEvent.class);
        this.configuration = configuration;
        this.eventTopicService = eventTopicService;
        this.resourceService = resourceService;
        this.objectMapper = objectMapper;
    }

    public RequestFintEvent sendEvent(String resourceName, Object resourceData, OperationType operationType) {
        FintResource fintResource = resourceService.mapResourceAndLinks(resourceName, resourceData);
        log.info("OK: {}", fintResource.toString());
        RequestFintEvent requestFintEvent = createRequestFintEvent(resourceName, fintResource, operationType);
        String eventName = createEventName(requestFintEvent);
        EventTopicNameParameters eventTopicNameParameters = EventTopicNameParameters.builder().eventName(eventName).build();

        ensureTopicIfItDoesntExist(eventName, eventTopicNameParameters);
        log.info("Sending event-id: {} - {}", requestFintEvent.getCorrId(), eventName);
        eventProducer.send(createProducerRecord(requestFintEvent.getCorrId(), eventTopicNameParameters, requestFintEvent));
        return requestFintEvent;
    }

    private EventProducerRecord<RequestFintEvent> createProducerRecord(String key, EventTopicNameParameters eventTopicNameParameters, RequestFintEvent requestFintEvent) {
        return EventProducerRecord.<RequestFintEvent>builder()
                .key(key)
                .topicNameParameters(eventTopicNameParameters)
                .value(requestFintEvent)
                .build();
    }

    private void ensureTopicIfItDoesntExist(String eventName, EventTopicNameParameters eventTopicNameParameters) {
        if (!topics.contains(eventName)) {
            log.info("Ensuring event topic: {}", eventName);
            eventTopicService.ensureTopic(eventTopicNameParameters, RETENTION_TIME_MS);
            topics.add(eventName);
        }
    }

    private RequestFintEvent createRequestFintEvent(String resourceName, Object resourceData, OperationType operationType) {
        return RequestFintEvent.builder()
                .corrId(UUID.randomUUID().toString())
                .domainName(configuration.getDomain())
                .packageName(configuration.getPackageName())
                .orgId(configuration.getOrgId())
                .created(System.currentTimeMillis())
                .resourceName(resourceName)
                .value(convertToJson(resourceData))
                .operationType(operationType)
                .build();
    }

    private String convertToJson(Object resource) {
        try {
            return objectMapper.writer().writeValueAsString(resource);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String createEventName(RequestFintEvent requestFintEvent) {
        return "%s-%s-%s-request".formatted(
                configuration.getDomain(),
                configuration.getPackageName(),
                requestFintEvent.getResourceName()
        );
    }

}
