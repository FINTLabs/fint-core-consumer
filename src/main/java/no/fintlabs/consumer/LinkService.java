package no.fintlabs.consumer;

import jakarta.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class LinkService {

    private final static int MAXIMUM_PLACEHOLDER_LENGTH = 500;

    @Value("${fint.relation.base-url}")
    private String baseUrl;

    private final ConsumerConfiguration configuration;
    private final ReflectionService reflectionService;

    public FintResources toResources(String resourceName, Stream<FintResource> stream, int offset, int size, int totalItems) {
        FintResources fintResources = new FintResources();
        stream.forEach(fintResources::addResource);
        return fintResources;
    }

    private void generateRelationLinks(String resourceName, FintResource resource) {
        resource.getLinks().forEach((relationName, links) -> {
            links.forEach(link -> {
                if (Objects.nonNull(link)) {
                    // TODO: Det finnes tilfeller hvor baseurl blir satt manuelt til ett annet miljø, undersøk dette videre
                    link.setVerdi("%s/%s/%s".formatted(baseUrl, getRelationUrl(resourceName, relationName), link.getHref()));
                } else {
                    log.error("A link is null");
                }
            });
        });
    }

    private void generateSelfLinks(String resourceName, FintResource resource) {
        String[] selfHrefs = createSelfHrefs(resourceName, resource);
        if (selfHrefs.length < 1) {
            log.error("Resource has no selfLinks!: {}", resource);
            return;
        }

        for (String selfHref : selfHrefs) {
            resource.addSelf(Link.with(selfHref));
        }
    }

    private String getRelationUrl(String resourceName, String relationName) {
        return reflectionService.getResources().get(resourceName).relationLinks().get(relationName);
    }

    private void removePlaceholders(String resourceName, FintResource fintResource) {
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
            log.error("Link: {} exceeds maximum length: {}", createSelfHref(resourceName, fintresource), href);
            throw new IllegalArgumentException("The href must not exceed %s characters.".formatted(MAXIMUM_PLACEHOLDER_LENGTH));
        }

        for (int i = href.length() - 1; i >= 0; i--) {
            if (href.charAt(i) == '/') {
                count++;
                if (count == 2) {
                    link.setVerdi(href.substring(i+1));
                    return;
                }
            }
        }

        log.error("Link: {} is missing path segments: {}", createSelfHref(resourceName, fintresource), href);
        throw new IllegalArgumentException("The href must contain at least two path segments.");
    }

    private String[] createSelfHrefs(String resourceName, FintResource resource) {
        return resource.getIdentifikators().entrySet().stream()
                .filter(entrySet -> entrySet.getValue().getIdentifikatorverdi() != null)
                .map(entrySet -> String.format("%s/%s/%s/%s/%s/%s",
                        baseUrl,
                        configuration.getDomain(),
                        configuration.getPackageName(),
                        resourceName,
                        entrySet.getKey().toLowerCase(),
                        entrySet.getValue().getIdentifikatorverdi()))
                .toArray(String[]::new);
    }

    @Nullable
    public String createSelfHref(String resourceName, FintResource resource) {
        return resource.getIdentifikators().entrySet().stream()
                .filter(entrySet -> entrySet.getValue().getIdentifikatorverdi() != null)
                .findFirst()
                .map(entrySet -> String.format("%s/%s/%s/%s/%s/%s",
                        baseUrl,
                        configuration.getDomain(),
                        configuration.getPackageName(),
                        resourceName,
                        entrySet.getKey().toLowerCase(),
                        entrySet.getValue().getIdentifikatorverdi()))
                .orElse(null);
    }

    public String createSelfHref(RequestFintEvent requestFintEvent) {
        return "%s/%s/%s/%s/status/%s".formatted(
                baseUrl,
                configuration.getDomain(),
                configuration.getPackageName(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getCorrId()
        );
    }

    public void mapLinks(String resourceName, FintResource resource) {
        resource.getLinks().put("self", new ArrayList<>());
        generateSelfLinks(resourceName, resource);
        removePlaceholders(resourceName, resource);
        generateRelationLinks(resourceName, resource);
    }
}
