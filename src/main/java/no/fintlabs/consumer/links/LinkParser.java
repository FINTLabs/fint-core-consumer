package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkError;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class LinkParser {

    private final LinkValidator linkValidator;

    // httpasdfasdfjaisdf/idField/ifValue

    public void removePlaceholders(String resourceName, FintResource fintResource, List<LinkError> linkErrors) {
        fintResource.getLinks().forEach((relationName, links) -> {
            if (!relationName.equals("self")) {
                if (links != null) {
                    processLinks(resourceName, relationName, links, linkErrors);
                } else {
                    fintResource.getLinks().put(relationName, new ArrayList<>());
                    linkErrors.add(new LinkError("The links of relation %s were null (created a new ArrayList)".formatted(relationName)));
                }
            }
        });
    }

    private void processLinks(String resourceName, String relationName, List<Link> links, List<LinkError> exceptions) {
        for (Link link : links) {
            if (linkValidator.validLink(link, exceptions)) {
                String[] linkSegments = link.getHref().toLowerCase().split("/");
                if (linkValidator.segmentsIsValid(linkSegments, exceptions)) {
                    if (linkValidator.validateIdField(resourceName, relationName, getIdFieldSegment(linkSegments), exceptions)) {
                        // TODO: Fødselsnummer hashing hvis idField er "fodselsnummer"?
                        link.setVerdi(getIdFieldAndIdValueUri(linkSegments));
                    }
                }
            }
        }
    }

    private String getIdFieldAndIdValueUri(String[] linkSegments) {
        String idFieldSegment = getIdFieldSegment(linkSegments);
        return "%s/%s".formatted(idFieldSegment, linkSegments[linkSegments.length - 1]);
    }

    private String getIdFieldSegment(String[] linkSegments) {
        return linkSegments[linkSegments.length - 2];
    }

}
