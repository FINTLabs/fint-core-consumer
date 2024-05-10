package no.fintlabs.consumer.resource;

import no.fintlabs.consumer.exception.EventFailedException;
import no.fintlabs.consumer.exception.EventRejectedException;
import no.fintlabs.consumer.exception.IdentificatorNotFoundException;
import no.fintlabs.consumer.exception.ResourceNotFoundException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class ResourceExceptionHandler {

    @ExceptionHandler(EventRejectedException.class)
    public ResponseEntity<?> eventRejected(EventRejectedException ex) {
        return ResponseEntity.badRequest().body(ex.getRejectReason());
    }

    @ExceptionHandler(EventFailedException.class)
    public ResponseEntity<?> eventFailed(EventFailedException ex) {
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

}
