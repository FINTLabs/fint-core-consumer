package no.fintlabs.consumer.exception.event;

import lombok.Getter;

@Getter
public class EventRejectedException extends EventException {

    public EventRejectedException(String corrId, String message) {
        super(corrId, message);
    }

}
