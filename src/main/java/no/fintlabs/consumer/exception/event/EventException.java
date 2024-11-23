package no.fintlabs.consumer.exception.event;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class EventException extends RuntimeException {

    private final String corrId;
    private final String message;

}
