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
        int count = 0;
        int endIndex = href.length();

        if (endIndex > MAXIMUM_PLACEHOLDER_LENGTH) {
            throw new LinkException("Resource exceeds maximum length of %s characters".formatted(MAXIMUM_PLACEHOLDER_LENGTH), href);
        }

        for (int i = href.length() - 1; i >= 0; i--) {
            if (href.charAt(i) == '/') {
                count++;
                if (count == 2) {
                    link.setVerdi(href.substring(i + 1));
                    return;
                }
            }
        }

        throw new LinkException("Resource doesnt contain enough path segments (2)", href);
    }

}
