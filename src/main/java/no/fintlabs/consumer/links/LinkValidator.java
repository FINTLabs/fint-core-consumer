package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LinkValidator {

    private final static int MAXIMUM_PLACEHOLDER_LENGTH = 500;
    private final RelationLinkIdFieldValidator relationLinkIdFieldValidator;

    public void validateLinks(String resourceName, String relationName, Link link) {
        String href = link.getHref();
        String[] segments = href.split("/");
        int segmentsLength = segments.length;

        if (href.length() > MAXIMUM_PLACEHOLDER_LENGTH) {
            throw new LinkException("Resource exceeds maximum length of %s characters".formatted(MAXIMUM_PLACEHOLDER_LENGTH), href);
        } else if (segmentsLength < 2) {
            throw new LinkException("Resource doesn't contain enough path segments (2)", href);
        }

        String idField = segments[segmentsLength - 2];
        if (!relationLinkIdFieldValidator.relationContainsIdField(resourceName, relationName, idField)) {
            throw new LinkException("Identificator field: %s does not match the id fields for this resource's relation: %s - %s".
                    formatted(idField, resourceName, relationName), href);
        }
    }
}
