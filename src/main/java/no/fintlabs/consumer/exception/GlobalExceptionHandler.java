package no.fintlabs.consumer.exception;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.kafka.ConsumerErrorPublisher;
import no.fintlabs.status.models.error.ConsumerError;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;


@ControllerAdvice
@RequiredArgsConstructor
@Slf4j
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private final ConsumerErrorPublisher consumerErrorPublisher;
    private final ConsumerConfiguration configuration;

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleExceptions(Exception ex) {
        logger.error("Caught in global exception handler:", ex);
        consumerErrorPublisher.publish(ConsumerError.fromException(
                ex,
                configuration.getDomain(),
                configuration.getPackageName(),
                configuration.getOrgId()
        ));
        return ResponseEntity.internalServerError().build();
    }

}