package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Stream;


@Slf4j
@Service
@RequiredArgsConstructor
public class LinkService {

    private final ConsumerConfiguration config;
    private final ReflectionService reflectionService;
    private final LinkUtils linkUtils;
    private final LinkParser linkParser;

    public FintResources toResources(String resourceName, Stream<FintResource> stream, int offset, int size, int totalItems) {
        FintResources fintResources = new FintResources();
        stream.forEach(fintResources::addResource);
        addPagination(resourceName, fintResources, offset, size, totalItems);
        return fintResources;
    }

    protected void addPagination(String resourceName, FintResources resources, int offset, int size, int totalItems) {
        String baseUri = self(resourceName);
        UriComponentsBuilder baseBuilder = UriComponentsBuilder.fromUriString(baseUri);

        if (size > 0) {
            addLink(resources, "self", baseBuilder, offset, size);

            if (offset > 0) {
                int prevOffset = Math.max(0, offset - size);
                addLink(resources, "prev", baseBuilder, prevOffset, size);
            }

            if (offset + size < totalItems) {
                int nextOffset = offset + size;
                addLink(resources, "next", baseBuilder, nextOffset, size);
            }
        } else {
            resources.addSelf(Link.with(baseUri));
        }

        resources.setOffset(offset);
        resources.setTotalItems(totalItems);
    }

    private void addLink(FintResources resources, String rel, UriComponentsBuilder builder, int offset, int size) {
        String uri = builder.replaceQueryParam("offset", offset)
                .replaceQueryParam("size", size)
                .toUriString();

        switch (rel) {
            case "self":
                resources.addSelf(Link.with(uri));
                break;
            case "prev":
                resources.addPrev(Link.with(uri));
                break;
            case "next":
                resources.addNext(Link.with(uri));
                break;
        }
    }


    public String self(String resourceName) {
        return "%s/%s".formatted(config.getComponentUrl(), resourceName);
    }

    private void generateRelationLinks(String resourceName, FintResource resource) {
        resource.getLinks().forEach((relationName, links) -> {
            if (!relationName.equals("self"))
                links.forEach(link -> {
                    if (Objects.nonNull(link)) {
                        // TODO: Det finnes tilfeller hvor baseurl blir satt manuelt til ett annet miljø, undersøk dette videre
                        link.setVerdi("%s/%s/%s".formatted(config.getBaseUrl(), getRelationUrl(resourceName, relationName), link.getHref()));
                    } else {
                        log.error("A link is null");
                    }
                });
        });
    }

    private void generateSelfLinks(String resourceName, FintResource resource) {
        String[] selfHrefs = linkUtils.createSelfHrefs(resourceName, resource);
        if (selfHrefs.length < 1) {
            log.error("Resource has no selfLinks!: {}", resource);
            return;
        }

        for (String selfHref : selfHrefs) {
            log.info(selfHref);
            resource.addSelf(Link.with(selfHref));
        }
    }

    private String getRelationUrl(String resourceName, String relationName) {
        return reflectionService.getResources().get(resourceName).relationLinks().get(relationName);
    }

    public void mapLinks(String resourceName, FintResource resource) {
        resource.getLinks().put("self", new ArrayList<>());
        generateSelfLinks(resourceName, resource);
        linkParser.removePlaceholders(resourceName, resource);
        generateRelationLinks(resourceName, resource);
    }

}
