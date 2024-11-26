package no.fintlabs.consumer.exception.event;

import lombok.Getter;


@Getter
public class EventNotFoundException extends EventException {

    public EventNotFoundException(String corrId, String message) {
        super(corrId, message);
    }

}
