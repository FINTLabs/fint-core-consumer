package no.fintlabs.consumer.links.validator;

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
    private final ResourceContext resourceContext;

    public RelationLinkIdFieldValidator(ResourceContext resourceContext) {
        this.resourceContext = resourceContext;
        setresourceLinkIds();
    }

    public boolean relationContainsIdField(String resourceName, String relationName, String idField) {
        return resourceLinkIdMap.get(resourceName).get(relationName).contains(idField);
    }

    private void setresourceLinkIds() {
        resourceContext.getResources().forEach(resource -> {
            resourceLinkIdMap.put(resource.name(), new HashMap<>());
            resource.relations().forEach(fintRelation -> {
                        resourceLinkIdMap.get(resource.name()).put(
                                fintRelation.getName().toLowerCase(),
                                convertIdFieldsToLowercase(fintRelation)
                        );
                    }
            );
        });
    }

    private Set<String> convertIdFieldsToLowercase(FintRelation fintRelation) {
        return resourceContext.getRelation(fintRelation.getPackageName()).idFields().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

}
