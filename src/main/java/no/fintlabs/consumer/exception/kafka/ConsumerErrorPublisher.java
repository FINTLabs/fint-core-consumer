package no.fintlabs.consumer.exception.kafka;

import lombok.extern.slf4j.Slf4j;
import no.novari.kafka.producing.ParameterizedProducerRecord;
import no.novari.kafka.producing.ParameterizedTemplate;
import no.novari.kafka.producing.ParameterizedTemplateFactory;
import no.novari.kafka.topic.EventTopicService;
import no.novari.kafka.topic.configuration.EventCleanupFrequency;
import no.novari.kafka.topic.configuration.EventTopicConfiguration;
import no.novari.kafka.topic.name.EventTopicNameParameters;
import no.novari.kafka.topic.name.TopicNamePrefixParameters;
import no.fintlabs.status.models.error.ConsumerError;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

@Slf4j
@Service
public class ConsumerErrorPublisher {

    private final ParameterizedTemplate<ConsumerError> eventProducer;
    private final EventTopicNameParameters eventName;
    private static final Duration RETENTION_TIME = Duration.ofDays(7);
    private static final int PARTITIONS = 1;

    public ConsumerErrorPublisher(
            ParameterizedTemplateFactory parameterizedTemplateFactory,
            EventTopicService eventTopicService
    ) {
        this.eventProducer = parameterizedTemplateFactory.createTemplate(ConsumerError.class);
        this.eventName = createEventName();
        eventTopicService.createOrModifyTopic(
                eventName,
                EventTopicConfiguration
                        .stepBuilder()
                        .partitions(PARTITIONS)
                        .retentionTime(RETENTION_TIME)
                        .cleanupFrequency(EventCleanupFrequency.NORMAL)
                        .build()
        );
    }

    public void publish(ConsumerError consumerError) {
        log.info("Publishing consumer-error to Kafka!");
        eventProducer.send(
                ParameterizedProducerRecord.<ConsumerError>builder()
                        .key(UUID.randomUUID().toString())
                        .topicNameParameters(eventName)
                        .value(consumerError)
                        .build()
        );
    }

    private EventTopicNameParameters createEventName() {
        return EventTopicNameParameters.builder()
                .topicNamePrefixParameters(
                        TopicNamePrefixParameters
                                .stepBuilder()
                                .orgId("fintlabs-no")
                                .domainContextApplicationDefault()
                                .build()
                )
                .eventName("consumer-error")
                .build();
    }

}
