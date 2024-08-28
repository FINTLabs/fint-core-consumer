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

    private final LinkUtils linkUtils;
    private final LinkErrorProducer linkErrorProducer;
    private final LinkValidator linkValidator;

    public void removePlaceholders(String resourceName, FintResource fintResource) {
        List<LinkException> exceptions = new ArrayList<>();

        fintResource.getLinks().forEach((relationName, links) -> {
            if (!relationName.equals("self")) {
                try {
                    links.forEach(link -> {
                        linkValidator.validateLinks(resourceName, relationName, link);
                        removePlaceholder(link);
                    });
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
        String[] segments = link.getHref().split("/");
        link.setVerdi("%s/%s".formatted(segments[segments.length - 2], segments[segments.length - 1]));
    }

}
