package no.fintlabs.consumer.exception;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public record LinkError(String errorMessage) {

    public LinkError(String errorMessage) {
        this.errorMessage = errorMessage;
        log.error(errorMessage);
    }

}
