package no.fintlabs.consumer.exception.kafka;

import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.kafka.event.EventProducer;
import no.fintlabs.kafka.event.EventProducerFactory;
import no.fintlabs.kafka.event.EventProducerRecord;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import no.fintlabs.kafka.event.topic.EventTopicService;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

@Service
public class ConsumerErrorPublisher {

    private final EventProducer<ConsumerError> eventProducer;
    private final EventTopicNameParameters eventName;

    public ConsumerErrorPublisher(EventProducerFactory eventProducerFactory, ConsumerConfiguration configuration, EventTopicService eventTopicService) {
        this.eventProducer = eventProducerFactory.createProducer(ConsumerError.class);
        this.eventName = createEventName(configuration);
        eventTopicService.ensureTopic(eventName, Duration.ofDays(7).toMillis());
    }

    public void publish(ConsumerError consumerError) {
        eventProducer.send(
                EventProducerRecord.<ConsumerError>builder()
                        .key(UUID.randomUUID().toString())
                        .topicNameParameters(eventName)
                        .value(consumerError)
                        .build()
        );
    }

    private EventTopicNameParameters createEventName(ConsumerConfiguration configuration) {
        return EventTopicNameParameters.builder()
                .orgId(configuration.getOrgId().replace(".", "-"))
                .eventName("consumer-error")
                .build();
    }

}
