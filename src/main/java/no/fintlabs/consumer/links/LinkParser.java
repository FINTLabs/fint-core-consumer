package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkException;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class LinkParser {

    private final static int MAXIMUM_PLACEHOLDER_LENGTH = 500;
    private final LinkUtils linkUtils;
    private final LinkErrorProducer linkErrorProducer;
    private final Map<String, Set<String>> validIdentificatorFields = new HashMap<>();

    public void removePlaceholders(String resourceName, FintResource fintResource) {
        List<LinkException> exceptions = new ArrayList<>();

        fintResource.getLinks().forEach((relationName, links) -> {
            if (!relationName.equals("self")) {
                try {
                    links.forEach(link -> {
//                        validateLink(link);
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

    private void validateLink(String linkName, Link link) {
        String href = link.getHref();
        String[] segments = href.split("/");
        int segmentsLength = segments.length;

        if (href.length() > MAXIMUM_PLACEHOLDER_LENGTH) {
            throw new LinkException("Resource exceeds maximum length of %s characters".formatted(MAXIMUM_PLACEHOLDER_LENGTH), href);
        } else if (segmentsLength < 2) {
            throw new LinkException("Resource doesn't contain enough path segments (2)", href);
        } else if (identificatorFieldIsNotValid(linkName, segments[segmentsLength - 2])) {
            throw new LinkException(
                    "Identificator field: %s does not match the id fields for this resource: %s".formatted(segments[segmentsLength - 2], validIdentificatorFields.get(linkName).toString()),
                    href
            );
        }
    }

    private boolean identificatorFieldIsNotValid(String linkName, String identificatorField) {
        return validIdentificatorFields.get(linkName).contains(identificatorField);
    }

    private void removePlaceholder(Link link) {
        String[] segments = link.getHref().split("/");
        link.setVerdi("%s/%s".formatted(segments[segments.length - 2], segments[segments.length - 1]));
    }

}
