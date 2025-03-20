package no.fintlabs.consumer.exception;

import lombok.RequiredArgsConstructor;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.kafka.ConsumerError;
import no.fintlabs.consumer.exception.kafka.ConsumerErrorPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;


@ControllerAdvice
@RequiredArgsConstructor
public class GlobalExceptionHandler {

    private final ConsumerErrorPublisher consumerErrorPublisher;
    private final ConsumerConfiguration configuration;

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleExceptions(Exception ex) {
        consumerErrorPublisher.publish(ConsumerError.from(ex, configuration));
        return ResponseEntity.internalServerError().build();
    }

}
