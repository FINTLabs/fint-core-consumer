package no.fintlabs.consumer.exception;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public record LinkError(
        String relationName,
        String errorMessage,
        String linkHref
) {

    public LinkError(String relationName, String errorMessage, String linkHref) {
        this.relationName = relationName;
        this.errorMessage = errorMessage;
        this.linkHref = linkHref;
        log.debug("relation: {} error: {} linkHref: {}", relationName, errorMessage, linkHref);
    }

}
