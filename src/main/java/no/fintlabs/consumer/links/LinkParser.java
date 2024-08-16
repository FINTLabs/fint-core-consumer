package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkException;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class LinkParser {

    private final static int MAXIMUM_PLACEHOLDER_LENGTH = 500;
    private final LinkUtils linkUtils;
    private final LinkErrorProducer linkErrorProducer;

    public void removePlaceholders(String resourceName, FintResource fintResource) {
        List<LinkException> exceptions = new ArrayList<>();

        fintResource.getLinks().forEach((relationName, links) -> {
            if (!relationName.equals("self")) {
                try {
                    links.forEach(this::removePlaceholder);
                } catch (LinkException linkException) {
                    exceptions.add(linkException);
                }
            }
        });

        if (!exceptions.isEmpty()) {
            linkErrorProducer.publishErrors(linkUtils.createFirstSelfHref(resourceName, fintResource), exceptions);
        }
    }

    private void removePlaceholder(Link link) {
        String href = link.getHref();
        int endIndex = href.length();

        if (endIndex > MAXIMUM_PLACEHOLDER_LENGTH) {
            throw new LinkException("Resource exceeds maximum length of %s characters".formatted(MAXIMUM_PLACEHOLDER_LENGTH), href);
        }

        String[] segments = href.split("/");

        if (segments.length < 2) {
            throw new LinkException("Resource doesn't contain enough path segments (2)", href);
        }

        String result;
        if (segments.length == 2) {
            result = segments[0] + "/" + segments[1];
        } else {
            result = segments[segments.length - 2] + "/" + segments[segments.length - 1];
        }

        link.setVerdi(result);
    }

}
