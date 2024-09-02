package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkError;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class LinkValidator {

    private final RelationLinkIdFieldValidator relationLinkIdFieldValidator;
    private final LinkRelations linkRelations;

    public boolean segmentsIsValid(String[] linkSegments, List<LinkError> linkErrors) {
        int segmentsLength = linkSegments.length;

        if (segmentsLength <= 1) {
            linkErrors.add(new LinkError("Link does not contain enough segments after splitting, 2 is required"));
            return false;
        }

        String idFieldSegment = linkSegments[segmentsLength - 2];
        String idValue = linkSegments[segmentsLength - 1];

        // TODO: valider hvis begge er tomme
        // TODO: hvis link slutter med "/" så kan idValue bli tom selv om den er satt

        if (idFieldSegment.isEmpty()) {
            linkErrors.add(new LinkError("IdField segment in Link is empty"));
            if (idValue.isEmpty()) {
                linkErrors.add(new LinkError("IdValue segment in Link is empty"));
            }
            return false;
        }

        if (idValue.isEmpty()) {
            linkErrors.add(new LinkError("IdValue segment in Link is empty"));
            return false;
        }

        return true;
    }

    public boolean validateIdField(String resourceName, String relationName, String idFieldSegment, List<LinkError> linkErrors) {
        if (!relationLinkIdFieldValidator.relationContainsIdField(resourceName, relationName, idFieldSegment)) {
            linkErrors.add(new LinkError("IdField segment in Link does not match the relation's valid IdFields: %s - %s".formatted(resourceName, idFieldSegment)));
            return false;
        }
        return true;
    }

    public boolean validLink(Link link, List<LinkError> linkErrors) {
        if (link != null) {
            if (link.getHref() == null) {
                link.setVerdi("");
                linkErrors.add(new LinkError("href of Link was null (set it to empty string)"));
                return false;
            }
            return true;
        }
        // set new Link
        linkErrors.add(new LinkError("Link is null"));
        return false;
    }

    public void checkIfRequiredRelationsIsSet(String resourceName, FintResource resource, ArrayList<LinkError> linkErrors) {
        linkRelations.getRequiredRelationNamesOfResource(resourceName).forEach(relationName -> {
            if (!resource.getLinks().containsKey(relationName)) {
                linkErrors.add(new LinkError("Required relation: %s is missing".formatted(relationName)));
            }
        });
    }
}
