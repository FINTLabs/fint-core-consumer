package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintLinks;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class LinkParser {

    private final LinkValidator linkValidator;
    private final ResourceContext resourceContext;

    public void removeNulls(FintLinks resource) {
        resource.getLinks().entrySet().removeIf(entry -> entry.getValue() == null);

        resource.getLinks().forEach((relationName, links) -> {
            links.removeIf(Objects::isNull);
            links.removeIf(link -> link.getHref() == null);
        });

        resource.getLinks().entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    public void removePlaceholders(String resourceName, FintResource fintResource, List<LinkError> linkErrors) {
        fintResource.getLinks().forEach((relationName, links) -> {
            if (!relationName.equals("self") && resourceContext.isNotFintReference(resourceName, relationName) && resourceContext.relationExists(resourceName, relationName)) {
                processLinks(resourceName, relationName, links, linkErrors);
            }
        });
    }

    private void processLinks(String resourceName, String relationName, List<Link> links, List<LinkError> exceptions) {
        for (Link link : links) {
            String[] linkSegments = link.getHref().split("/");
            if (linkValidator.segmentsIsValid(linkSegments, exceptions)) {
                String idField = getIdFieldSegment(linkSegments).toLowerCase();
                String idValue = getIdValueSegment(linkSegments);

                if (linkValidator.validateIdField(resourceName, relationName, idField, exceptions)) {
                    link.setVerdi("%s/%s".formatted(idField, idValue));
                }
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
