package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

@Slf4j
@Component
@RequiredArgsConstructor
public class LinkGenerator {

    private final ConsumerConfiguration configuration;
    private final ResourceContext resourceContext;

    public void resetSelfLinks(String resourceName, FintResource resource) {
        resource.getLinks().put("self", new ArrayList<>());

        String[] selfHrefs = createSelfHrefs(resourceName, resource);
        if (selfHrefs.length < 1) {
            log.error("Resource has no selfLinks: %s - %s".formatted(resourceName, resource));
        }

        for (String selfHref : selfHrefs) {
            resource.addSelf(Link.with(selfHref));
        }
    }

    private String[] createSelfHrefs(String resourceName, FintResource resource) {
        return resource.getIdentifikators().entrySet().stream()
                .filter(entrySet -> entrySet.getValue() != null)
                .filter(entrySet -> entrySet.getValue().getIdentifikatorverdi() != null)
                .map(entrySet -> String.format("%s/%s/%s/%s",
                        configuration.getComponentUrl(),
                        resourceName,
                        entrySet.getKey().toLowerCase(),
                        entrySet.getValue().getIdentifikatorverdi()))
                .toArray(String[]::new);
    }

    public String createRelationLink(String resourceName, String relationName, String href) {
        return "%s/%s/%s".formatted(
                configuration.getBaseUrl(),
                resourceContext.getRelationUri(resourceName, relationName),
                getLastTwoSegments(href)
        );
    }

    private String getLastTwoSegments(String href) {
        String[] split = href.split("/");

        if (split.length < 2) {
            log.error("Link href is invalid: {}", href);
            return href;
        }

        return "%s/%s".formatted(split[split.length - 2], split[split.length - 1]);
    }

}
