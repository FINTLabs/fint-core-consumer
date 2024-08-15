package no.fintlabs.consumer.kafka;

import no.fintlabs.kafka.event.error.Error;
import no.fintlabs.kafka.event.error.ErrorCollection;
import no.fintlabs.kafka.event.error.ErrorEventProducer;
import no.fintlabs.kafka.event.error.ErrorEventProducerRecord;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class LinkErrorProducer {

    private final ErrorEventProducer eventProducer;
    private final ErrorEventTopicNameParameters errorEventTopicName;

    public LinkErrorProducer(ErrorEventProducer eventProducer) {
        this.eventProducer = eventProducer;
        this.errorEventTopicName = ErrorEventTopicNameParameters.builder()
                .errorEventName("consumer-link")
                .build();

    }

    public void publishError(String resourceLink, String errorLink, String errorMessage) {
        eventProducer.send(
                ErrorEventProducerRecord.builder()
                        .topicNameParameters(errorEventTopicName)
                        .errorCollection(createErrorCollection(resourceLink, errorLink, errorMessage))
                        .build()
        );
    }

    private ErrorCollection createErrorCollection(String resourceLink, String errorLink, String errorMessage) {
        ErrorCollection errorCollection = new ErrorCollection();

        errorCollection.setErrors(List.of(
                Error.builder()
                        .errorCode(errorMessage)
                        .args(Map.of(
                                "resourceLink", resourceLink,
                                "errorLink", errorLink
                        ))
                        .build()
        ));

        return errorCollection;
    }

}
