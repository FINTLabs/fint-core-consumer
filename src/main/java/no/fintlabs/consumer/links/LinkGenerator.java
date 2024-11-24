package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class LinkGenerator {

    private final ConsumerConfiguration configuration;
    private final ResourceContext resourceContext;

    public void resetAndGenerateSelfLinks(String resourceName, FintResource resource, List<LinkError> linkErrors) {
        resource.getLinks().put("self", new ArrayList<>());

        String[] selfHrefs = createSelfHrefs(resourceName, resource);
        if (selfHrefs.length < 1) {
            linkErrors.add(new LinkError("Resource has no selfLinks: %s - %s".formatted(resourceName, resource)));
        }

        for (String selfHref : selfHrefs) {
            resource.addSelf(Link.with(selfHref));
        }
    }

    public void generateRelationLinks(String resourceName, FintResource resource) {
        resource.getLinks().forEach((relationName, links) -> {
            if (!relationName.equals("self") && resourceContext.isNotFintReference(resourceName, relationName) && resourceContext.relationExists(resourceName, relationName))
                links.forEach(link -> link.setVerdi("%s/%s/%s".formatted(
                        configuration.getBaseUrl(),
                        resourceContext.getRelationUri(resourceName, relationName),
                        link.getHref()))
                );
        });
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

}
