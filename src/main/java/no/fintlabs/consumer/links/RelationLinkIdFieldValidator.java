package no.fintlabs.consumer.links;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintRelation;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
public class RelationLinkIdFieldValidator {

    private final Map<String, Map<String, Set<String>>> resourceLinkIdMap = new HashMap<>();

    public RelationLinkIdFieldValidator(ResourceContext resourceContext) {
        setresourceLinkIds(resourceContext);
    }

    public boolean relationContainsIdField(String resourceName, String relationName, String idField) {
        return resourceLinkIdMap.get(resourceName).get(relationName).contains(idField);
    }

    private void setresourceLinkIds(ResourceContext resourceContext) {
        resourceContext.getFintResourceInformationMap().forEach((resourceName, resourceInformation) -> {
            resourceInformation.relations().forEach(fintRelation ->
                    resourceLinkIdMap.put(resourceName, Map.of(
                                    fintRelation.getName(),
                                    convertIdFieldsToLowercase(resourceContext, fintRelation)
                            )
                    )
            );
        });
    }

    private Set<String> convertIdFieldsToLowercase(ResourceContext resourceContext, FintRelation fintRelation) {
        return resourceContext.getFintRelationInformationMap().get(fintRelation.getPackageName()).idFields().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

}
