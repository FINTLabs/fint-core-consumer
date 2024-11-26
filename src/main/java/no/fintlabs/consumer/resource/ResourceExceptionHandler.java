package no.fintlabs.consumer.resource;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.exception.event.EventFailedException;
import no.fintlabs.consumer.exception.event.EventNotFoundException;
import no.fintlabs.consumer.exception.event.EventRejectedException;
import no.fintlabs.consumer.exception.resource.IdentificatorNotFoundException;
import no.fintlabs.consumer.exception.resource.ResourceNotFoundException;
import no.fintlabs.consumer.exception.resource.ResourceNotWriteableException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@Slf4j
@ControllerAdvice
public class ResourceExceptionHandler {

    @ExceptionHandler(EventRejectedException.class)
    public ResponseEntity<?> eventRejected(EventRejectedException ex) {
        log.error(
                "Event Status: 400 BAD REQUEST, Corr-id: {}, Reason: {}, Error Message: {}",
                ex.getCorrId(),
                "Event was rejected",
                ex.getMessage()
        );
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(EventNotFoundException.class)
    public ResponseEntity<?> eventRejected(EventNotFoundException ex) {
        log.error(
                "Event Status: 404 NOT FOUND, Corr-id: {}, Reason: {}, Error Message: {}",
                ex.getCorrId(),
                "Event does not exist",
                ex.getMessage()
        );
        return ResponseEntity.notFound().build();
    }

    @ExceptionHandler(EventFailedException.class)
    public ResponseEntity<?> eventFailed(EventFailedException ex) {
        log.error(
                "Event Status: 500 INTERNAL SERVER ERROR, Corr-id: {}, Reason: {}, Error Message: {}",
                ex.getCorrId(),
                "Event failed",
                ex.getMessage()
        );
        return ResponseEntity.internalServerError().body(ex.getMessage());
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<?> resourceNotFound(ResourceNotFoundException ex) {
        return ResponseEntity.notFound().build();
    }

    @ExceptionHandler(IdentificatorNotFoundException.class)
    public ResponseEntity<?> identificatorNotFound(IdentificatorNotFoundException ex) {
        return ResponseEntity.notFound().build();
    }

    @ExceptionHandler(ResourceNotWriteableException.class)
    public ResponseEntity<?> resourceNotWriteable(ResourceNotWriteableException ex) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(ex.getMessage());
    }

}
