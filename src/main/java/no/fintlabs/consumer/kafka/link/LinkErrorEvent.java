package no.fintlabs.consumer.kafka.link;

import no.fintlabs.consumer.exception.LinkError;

import java.util.List;

public class LinkErrorEvent {
    private final List<String> resourceLinks;
    private final List<LinkError> linkErrors;

    public LinkErrorEvent(List<String> resourceLinks, List<LinkError> linkErrors) {
        this.resourceLinks = resourceLinks;
        this.linkErrors = linkErrors;
    }

}
