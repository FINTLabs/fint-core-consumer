package no.fintlabs.consumer.exception;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class ExceptionHandlerr {

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<?> resourceNotFound(ResourceNotFoundException ex) {
        return ResponseEntity.notFound().build();
    }

    @ExceptionHandler(IdentificatorNotFoundException.class)
    public ResponseEntity<?> identificatorNotFound(IdentificatorNotFoundException ex) {
        return ResponseEntity.notFound().build();
    }

}
