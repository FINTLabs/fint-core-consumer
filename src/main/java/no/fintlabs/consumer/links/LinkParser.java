package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.links.validator.LinkValidator;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class LinkParser {

    private final LinkValidator linkValidator;
    private final ResourceContext resourceContext;
    private final LinkGenerator linkGenerator;

    public void removeSelfLinks(FintResource resource) {
        resource.getLinks().remove("self");
    }

    public void processRelations(String resourceName, FintResource resource, List<LinkError> linkErrors) {
        resource.getLinks().entrySet().removeIf(
                entry -> shouldRemoveRelation(resourceName, entry.getKey(), entry.getValue(), linkErrors)
        );
    }

    private boolean shouldRemoveRelation(String resourceName, String relationName, List<Link> relationLinks, List<LinkError> linkErrors) {
        if (relationLinks == null) {
            linkErrors.add(new LinkError(relationName, "links list is null", null));
            return true;
        }

        boolean hasProcessableLink = processRelationLinks(resourceName, relationName, relationLinks, linkErrors);

        return !hasProcessableLink;
    }

    private boolean processRelationLinks(String resourceName, String relationName, List<Link> relationLinks, List<LinkError> linkErrors) {
        boolean hasProcessableLink = false;

        for (int i = relationLinks.size() - 1; i >= 0; i--) {
            Link link = relationLinks.get(i);

            if (link == null) {
                linkErrors.add(new LinkError(relationName, "Link is null", null));
                relationLinks.remove(i);
                continue;
            }

            if (link.getHref() == null) {
                linkErrors.add(new LinkError(relationName, "Href is null", null));
                relationLinks.remove(i);
                continue;
            }

            hasProcessableLink = true;
            processLink(resourceName, relationName, link, linkErrors);
        }

        return hasProcessableLink;
    }

    private void processLink(String resourceName, String relationName, Link link, List<LinkError> linkErrors) {
        if (resourceContext.notFintReference(resourceName, relationName)) {
            removePlaceholder(resourceName, relationName, link, linkErrors);
            linkGenerator.generateRelationLink(resourceName, relationName, link);
        }
    }

    private void removePlaceholder(String resourceName, String relationName, Link link, List<LinkError> linkErrors) {
        String[] linkSegments = link.getHref().split("/");
        if (linkValidator.segmentsIsValid(linkSegments)) {
            String idField = getIdFieldSegment(linkSegments).toLowerCase();
            String idValue = getIdValueSegment(linkSegments);

            if (linkValidator.validateIdField(resourceName, relationName, idField)) {
                link.setVerdi("%s/%s".formatted(idField, idValue));
            } else {
                linkErrors.add(new LinkError(relationName, "failed idField validation", link.getHref()));
            }
        } else {
            linkErrors.add(new LinkError(relationName, "failed linkSegments validation", link.getHref()));
        }
    }

    private String getIdFieldSegment(String[] linkSegments) {
        return linkSegments[linkSegments.length - 2];
    }

    private String getIdValueSegment(String[] linkSegments) {
        return linkSegments[linkSegments.length - 1];
    }

}
