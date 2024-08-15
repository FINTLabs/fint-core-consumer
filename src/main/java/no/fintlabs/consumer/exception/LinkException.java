package no.fintlabs.consumer.exception;

import lombok.Getter;

@Getter
public class LinkException extends RuntimeException {

    private final String errorValue;

    public LinkException(String message, String errorValue) {
        super(message);
        this.errorValue = errorValue;
    }
}
