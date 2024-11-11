package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.links.validator.LinkValidator;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.Iterator;
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

    public void processRelations(String resourceName, FintResource resource) {
        resource.getLinks().entrySet().removeIf(
                entry -> shouldRemoveRelation(resourceName, entry.getKey(), entry.getValue())
        );
    }

    private boolean shouldRemoveRelation(String resourceName, String relationName, List<Link> relationLinks) {
        if (relationLinks == null) {
            return true;
        }

        boolean hasProcessableLink = processRelationLinks(resourceName, relationName, relationLinks);

        return !hasProcessableLink;
    }

    private boolean processRelationLinks(String resourceName, String relationName, List<Link> relationLinks) {
        Iterator<Link> linkIterator = relationLinks.iterator();
        boolean hasProcessableLink = false;

        while (linkIterator.hasNext()) {
            Link link = linkIterator.next();

            if (link == null || link.getHref() == null) {
                linkIterator.remove();
            } else {
                hasProcessableLink = true;
                processLink(resourceName, relationName, link);
            }
        }

        return hasProcessableLink;
    }

    private void processLink(String resourceName, String relationName, Link link) {
        if (resourceContext.notFintReference(resourceName, relationName)) {
            removePlaceholder(resourceName, relationName, link);
            linkGenerator.generateRelationLink(resourceName, relationName, link);
        }
    }

    private void removePlaceholder(String resourceName, String relationName, Link link) {
        String[] linkSegments = link.getHref().split("/");
        if (linkValidator.segmentsIsValid(linkSegments)) {
            String idField = getIdFieldSegment(linkSegments).toLowerCase();
            String idValue = getIdValueSegment(linkSegments);

            if (linkValidator.validateIdField(resourceName, relationName, idField)) {
                link.setVerdi("%s/%s".formatted(idField, idValue));
            }
        }
    }

    private String getIdFieldSegment(String[] linkSegments) {
        return linkSegments[linkSegments.length - 2];
    }

    private String getIdValueSegment(String[] linkSegments) {
        return linkSegments[linkSegments.length - 1];
    }

}
