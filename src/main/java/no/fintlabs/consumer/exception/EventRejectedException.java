package no.fintlabs.consumer.exception;

import lombok.Getter;

@Getter
public class EventRejectedException extends RuntimeException {

    private final String rejectReason;

    public EventRejectedException(String errorMessage, String rejectReason) {
        super(errorMessage);
        this.rejectReason = rejectReason;
    }

}
