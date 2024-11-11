package no.fintlabs.consumer.links.validator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.links.LinkRelations;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class LinkValidator {

    private final RelationIdFieldCache relationIdFieldCache;
    private final LinkRelations linkRelations;

    public boolean segmentsIsValid(String[] linkSegments) {
        int segmentsLength = linkSegments.length;

        if (segmentsLength <= 1) {
            return false;
        }

        String idFieldSegment = linkSegments[segmentsLength - 2];
        String idValue = linkSegments[segmentsLength - 1];

        if (idFieldSegment.isEmpty()) {
            return false;
        }

        return !idValue.isEmpty();
    }

    public boolean validateIdField(String resourceName, String relationName, String idField) {
        return relationIdFieldCache.relationContainsIdField(resourceName, relationName, idField);
    }

    public void checkIfRequiredRelationsIsSet(String resourceName, FintResource resource, List<LinkError> linkErrors) {
        linkRelations.getRequiredRelationNames(resourceName).forEach(relationName -> {
            if (!resource.getLinks().containsKey(relationName)) {
                linkErrors.add(new LinkError("Required relation is missing: %s".formatted(relationName)));
            }
        });
    }
}
