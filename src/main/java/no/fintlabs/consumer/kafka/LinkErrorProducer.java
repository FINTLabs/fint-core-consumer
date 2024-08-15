package no.fintlabs.consumer.kafka;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.event.error.Error;
import no.fintlabs.kafka.event.error.ErrorCollection;
import no.fintlabs.kafka.event.error.ErrorEventProducer;
import no.fintlabs.kafka.event.error.ErrorEventProducerRecord;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicService;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

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

    public void publishError(String resourceLink, String errorLink, String errorMessage) {
        eventProducer.send(
                ErrorEventProducerRecord.builder()
                        .topicNameParameters(errorEventTopicName)
                        .errorCollection(createErrorCollection(resourceLink, errorLink, errorMessage))
                        .build()
        );
    }

    private ErrorCollection createErrorCollection(String resourceLink, String errorValue, String errorMessage) {
        ErrorCollection errorCollection = new ErrorCollection();

        errorCollection.setErrors(List.of(
                Error.builder()
                        .errorCode(errorMessage)
                        .args(Map.of(
                                "resourceLink", resourceLink,
                                "errorValue", errorValue
                        ))
                        .build()
        ));

        return errorCollection;
    }

}
