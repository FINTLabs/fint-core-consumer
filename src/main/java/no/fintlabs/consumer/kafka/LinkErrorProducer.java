package no.fintlabs.consumer.kafka;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.kafka.event.error.Error;
import no.fintlabs.kafka.event.error.ErrorCollection;
import no.fintlabs.kafka.event.error.ErrorEventProducer;
import no.fintlabs.kafka.event.error.ErrorEventProducerRecord;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicService;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class LinkErrorProducer {

    private final ErrorEventProducer eventProducer;
    private final ErrorEventTopicNameParameters errorEventTopicName;
    private final static Long DAY_IN_MILLISECONDS = 86400000L;

    public LinkErrorProducer(ErrorEventProducer eventProducer, ErrorEventTopicService errorEventTopicService) {
        this.eventProducer = eventProducer;
        this.errorEventTopicName = ErrorEventTopicNameParameters.builder()
                .errorEventName("consumer-link")
                .build();
        errorEventTopicService.ensureTopic(errorEventTopicName, DAY_IN_MILLISECONDS);
    }

    public void publishErrors(String resourceLink, List<LinkError> exceptions) {
        log.error("Publishing Link Error Collection of {} to Kafka", exceptions.size());
        eventProducer.send(
                ErrorEventProducerRecord.builder()
                        .headers(setResourceLinkInHeaders(resourceLink))
                        .topicNameParameters(errorEventTopicName)
                        .errorCollection(createErrorCollection(exceptions))
                        .build()
        );
    }

    private Headers setResourceLinkInHeaders(String resourceLink) {
        RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add("resource-link", resourceLink.getBytes());
        return recordHeaders;
    }

    private ErrorCollection createErrorCollection(List<LinkError> exceptions) {
        ErrorCollection errorCollection = new ErrorCollection();

        errorCollection.setErrors(
                exceptions.stream()
                        .map(linkException -> Error.builder()
                                .errorCode(linkException.errorMessage())
                                .build()
                        ).toList()
        );

        return errorCollection;
    }

}
