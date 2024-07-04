package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LinkParser {

    private final static int MAXIMUM_PLACEHOLDER_LENGTH = 500;

    private final LinkUtils linkUtils;

    public void removePlaceholders(String resourceName, FintResource fintResource) {
        fintResource.getLinks().forEach((relationName, links) -> {
            if (!relationName.equals("self")) {
                links.forEach(link -> removePlaceholder(resourceName, fintResource, link));
            }
        });
    }

    private void removePlaceholder(String resourceName, FintResource fintresource, Link link) {
        String href = link.getHref();
        int count = 0;
        int endIndex = href.length();

        if (endIndex > MAXIMUM_PLACEHOLDER_LENGTH) {
            log.error("Link: {} exceeds maximum length: {}", fintresource.getSelfLinks().getFirst().getHref(), href);
            throw new IllegalArgumentException("The href must not exceed %s characters.".formatted(MAXIMUM_PLACEHOLDER_LENGTH));
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

        log.error("Link: {} is missing path segments: {}", fintresource.getSelfLinks().getFirst().getHref(), href);
        throw new IllegalArgumentException("The href must contain at least two path segments.");
    }

}
