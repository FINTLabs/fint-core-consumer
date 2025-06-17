package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.links.nested.NestedLinkService;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;


@Slf4j
@Service
@RequiredArgsConstructor
public class LinkService {

    private final LinkPaginator linkPaginator;
    private final LinkGenerator linkGenerator;
    private final NestedLinkService nestedLinkService;
    private final ResourceContext resourceContext;

    public FintResources toResources(String resourceName, Stream<FintResource> resourceStream, int offset, int size, int totalItems) {
        Objects.requireNonNull(resourceStream, "resourceStream is null");

        FintResources fintResources = new FintResources();
        resourceStream
                .filter(Objects::nonNull)
                .forEach(fintResources::addResource);

        linkPaginator.addPagination(resourceName, fintResources, offset, size, totalItems);
        return fintResources;
    }

    public void mapLinks(String resourceName, FintResource resource) {
        resource.getLinks().remove("self");

        resource.getLinks().entrySet().removeIf(entry -> {
            processLinkList(resourceName, entry.getKey(), entry.getValue());
            return entry.getValue().isEmpty();
        });

        linkGenerator.resetSelfLinks(resourceName, resource);
        nestedLinkService.mapNestedLinks(resource);
    }

    private void processLinkList(String resourceName, String relationName, List<Link> links) {
        links.removeIf(link -> {
            if (link == null || link.getHref() == null)
                return true;

            link.setVerdi(processHref(resourceName, relationName, link.getHref()));
            return false;
        });
    }

    private String processHref(String resourceName, String relationName, String href) {
        if (href == null)
            return null;

        if (linkShouldBeProcessed(resourceName, relationName, href)) {
            return linkGenerator.createRelationLink(resourceName, relationName, href);
        } else return href;
    }

    private boolean linkShouldBeProcessed(String resourceName, String relationName, String href) {
        return resourceContext.relationExists(resourceName, relationName)
                && resourceContext.isNotFintReference(resourceName, relationName)
                && (isTemplated(href) || isRelative(href) || isNotLink(href));
    }

    private boolean isNotLink(String href) {
        return !href.startsWith("http");
    }

    private boolean isTemplated(String href) {
        return href.startsWith("${") && href.contains("}");
    }

    private boolean isRelative(String href) {
        return href.startsWith("/");
    }

}
