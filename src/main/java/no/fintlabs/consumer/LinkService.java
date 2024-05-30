package no.fintlabs.consumer;

import jakarta.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.larling.LarlingResource;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class LinkService {

    @Value("${fint.relation.base-url}")
    private String baseUrl;

    private final ConsumerConfiguration configuration;
    private final ReflectionService reflectionService;

    public FintResources toResources(String resourceName, Stream<LarlingResource> stream, int offset, int size, int totalItems) {
        FintResources fintResources = new FintResources();
        stream.map(resource -> toResource(resourceName, resource)).forEach(fintResources::addResource);
        return fintResources;
    }

    public FintResource toResource(String resourceName, FintResource resource) {
        mapLinks(resourceName, resource);
        return resource;
    }

    public void mapLinks(String resourceName, FintResource resource) {
        if (resource == null) {
            log.error("Resource is null!");
            return;
        }

        resource.getLinks().forEach((relationName, links) -> {
            if (Objects.nonNull(links)) {
                if (relationName.equals("self")) {
                    generateSelfLinks(links, resourceName, resource);
                } else {
                    generateRelationLinks(links, resourceName, relationName);
                }
            } else {
                log.error("");
            }
        });

        // TODO: Check out what nestedResources returns, and if resourceName is compatible
//        resource.getNestedResources().forEach(s -> mapLinks(resourceName, s));
    }

    private void generateRelationLinks(List<Link> links, String resourceName, String relationName) {
        links.forEach(link -> {
            if (Objects.nonNull(link)) {
                // TODO: Det finnes tilfeller hvor baseurl blir satt manuelt til ett annet miljø, undersøk dette videre
                link.setVerdi("%s/%s/%s".formatted(baseUrl, getRelationUrl(resourceName, relationName), link.getHref()));
            } else {
                log.error("");
            }
        });
    }

    private void generateSelfLinks(List<Link> links, String resourceName, FintResource resource) {
        // TODO: Test om å beholde self lenkene i objektet istedenfor å generere dem på nytt
        String[] selfHrefs = createSelfHrefs(resourceName, resource);
        if (selfHrefs == null || selfHrefs.length < 1) {
            log.error("Resource has no selfLinks!: {}", resource);
            return;
        }

        links.clear();
        for (String selfHref : selfHrefs) {
            links.add(Link.with(selfHref));
        }
    }

    private String getRelationUrl(String resourceName, String relationName) {
        return reflectionService.getResources().get(resourceName).relationLinks().get(relationName);
    }

    public void removePlaceholders(String resourceName, FintResource fintResource) {
        if (fintResource != null) {
            fintResource.getLinks().forEach((relationName, links) -> {
                if (relationName.equals("self")) {
                    links.clear();
                } else {
                    links.forEach(link -> removePlaceholder(resourceName, fintResource, link));
                }
            });
        }
    }

    private void removePlaceholder(String resourceName, FintResource fintresource, Link link) {
        if (link.getHref().startsWith("${")) {
            link.setVerdi(link.getHref().replaceAll("\\$\\{[^}]*}", ""));
        }

        // TODO: Legg til validering og logging av feil hvis brukeren har sendt inn noe vi ikke forventer men blir fortsatt tatt videre
        String[] parts = link.getHref().split("/");
        if (parts.length < 2) {
            log.error("Link: {} is missing path segments: {}", createSelfHref(resourceName, fintresource), link.getHref());
            throw new IllegalArgumentException("The href must contain at least two path segments.");
        }

        if (parts[0].isEmpty()) {
            parts = Arrays.copyOfRange(parts, 1, parts.length);
        }

        link.setVerdi(parts[parts.length - 2] + "/" + parts[parts.length - 1]);
    }

    @Nullable
    public String[] createSelfHrefs(String resourceName, FintResource resource) {
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

}
