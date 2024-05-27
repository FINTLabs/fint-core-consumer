package no.fintlabs.consumer;

import jakarta.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import no.fint.model.resource.FintLinks;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.larling.LarlingResource;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.Objects;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
public class LinkService {

    @Value("${fint.relation.base-url}")
    private String baseUrl;

    private final ConsumerConfiguration configuration;
    private final ReflectionService reflectionService;

    public FintResources toResources(String resourceName, Stream<LarlingResource> stream, int offset, int size, int totalItems) {
        FintResources fintResources = new FintResources();
//        stream.map(resource -> toResource(resourceName, resource)).forEach(fintResources::addResource);
        return fintResources;
    }

//    public FintResource toResource(String resourceName, FintResource resource) {
//        this.mapLinks(resourceName, resource);
//        if (resource.getSelfLinks() != null) {
//            resource.getSelfLinks().clear();
//        }
//
//        Stream var10000 = this.getAllSelfHrefs(resource).filter(StringUtils::isNotBlank);
//        FintLinkMapper var10001 = this.linkMapper;
//        Objects.requireNonNull(var10001);
//        var10000 = var10000.map(var10001::populateProtocol).map(Link::with);
//        Objects.requireNonNull(resource);
//        var10000.forEach(resource::addSelf);
//        return resource;
//    }

    public void mapLinks(String resourceName, FintLinks resource) {
        if (resource != null) {

            resource.getLinks().forEach((relationName, links) -> {
                if (Objects.nonNull(links)) {
                    links.forEach(link -> {
                        if (Objects.nonNull(link)) {
                            link.setVerdi("%s/%s/%s".formatted(baseUrl, getRelationUrl(resourceName, relationName), link.getHref()));
                        }
                    });
                }
            });

            // TODO: Check out what nestedResources returns, and if resourceName is compatible
            resource.getNestedResources().forEach(s -> mapLinks(resourceName, s));
        }

    }

    private String getRelationUrl(String resourceName, String relationName) {
        return reflectionService.getResources().get(resourceName).relationLinks().get(relationName);
    }


    public void removePlaceholders(FintResource fintResource) {
        if (fintResource != null) {
            fintResource.getLinks().forEach((resourceName, links) -> {
                links.forEach(this::removePlaceholder);
            });
        }
    }

    private void removePlaceholder(Link link) {
        link.setVerdi(link.getHref().replaceAll("\\$\\{[^}]*}", ""));
    }

    @Nullable
    public URI createSelfHref(String resourceName, FintResource resource) {
        return resource.getIdentifikators().entrySet().stream()
                .filter(e -> e.getValue().getIdentifikatorverdi() != null)
                .findFirst()
                .map(e -> URI.create(String.format("%s/%s/%s/%s/%s/%s",
                        baseUrl,
                        configuration.getDomain(),
                        configuration.getPackageName(),
                        resourceName,
                        e.getKey().toLowerCase(),
                        e.getValue().getIdentifikatorverdi())))
                .orElse(null);
    }

    public URI createSelfHref(RequestFintEvent requestFintEvent) {
        return URI.create("%s/%s/%s/%s/status/%s".formatted(
                baseUrl,
                configuration.getDomain(),
                configuration.getPackageName(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getCorrId()
        ));
    }

}
