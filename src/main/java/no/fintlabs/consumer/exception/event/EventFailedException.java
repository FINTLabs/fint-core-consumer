package no.fintlabs.consumer.exception.event;

import lombok.Getter;


@Getter
public class EventFailedException extends EventException {

    public EventFailedException(String corrId, String message) {
        super(corrId, message);
    }

}
