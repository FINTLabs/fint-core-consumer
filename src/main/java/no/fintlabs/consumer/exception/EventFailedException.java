package no.fintlabs.consumer.exception;

import lombok.Getter;

@Getter
public class EventFailedException extends RuntimeException {

    public EventFailedException(String errorMessage) {
        super(errorMessage);
    }

}
