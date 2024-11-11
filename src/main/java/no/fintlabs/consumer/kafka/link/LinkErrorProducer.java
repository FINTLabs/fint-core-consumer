package no.fintlabs.consumer.kafka.link;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.kafka.event.EventProducer;
import no.fintlabs.kafka.event.EventProducerFactory;
import no.fintlabs.kafka.event.EventProducerRecord;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class LinkErrorProducer {

    private final EventProducer<LinkErrorEvent> eventProducer;
    private final EventTopicNameParameters errorEventTopicName;

    public LinkErrorProducer(EventProducerFactory eventProducerFactory) {
        this.eventProducer = eventProducerFactory.createProducer(LinkErrorEvent.class);
        this.errorEventTopicName = EventTopicNameParameters.builder()
                .orgId("fintlabs-no")
                .eventName("link-error")
                .build();
    }

    public void publishErrors(List<String> resourceLinks, List<LinkError> linkErrors) {
        log.error("Publishing LinkErrorEvent containing: {} related to: {}", linkErrors.size(), resourceLinks);
        eventProducer.send(
                EventProducerRecord.<LinkErrorEvent>builder()
                        .topicNameParameters(errorEventTopicName)
                        .value(new LinkErrorEvent(resourceLinks, linkErrors))
                        .build()
        );
    }

}
