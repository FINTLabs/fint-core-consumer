package no.fintlabs.consumer.links.validator;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintRelation;
import no.fintlabs.reflection.model.FintResourceInformation;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class RelationIdFieldCache {

    private final Map<String, Map<String, Set<String>>> resourceLinkIdMap = new HashMap<>();
    private final ResourceContext resourceContext;

    @PostConstruct
    private void init() {
        setResourceLinkIds();
    }

    public boolean relationContainsIdField(String resourceName, String relationName, String idField) {
        Map<String, Set<String>> relationToIdFieldMap = resourceLinkIdMap.get(resourceName);
        if (relationToIdFieldMap != null) {
            Set<String> idFields = relationToIdFieldMap.get(relationName);
            if (idFields != null) {
                return idFields.contains(idField);
            } else {
                log.error("relationName does not exist in relationToIdFieldMap: {} - {}", resourceName, relationName);
            }
        } else {
            log.error("Resource does not exist in resourceLinkIdMap: {}", resourceName);
        }
        return false;
    }

    private void setResourceLinkIds() {
        resourceContext.getResources().forEach(resource -> {
            resourceLinkIdMap.put(resource.name(), new HashMap<>());
            resource.relations().forEach(fintRelation -> processRelation(resource, fintRelation));
        });
    }

    private void processRelation(FintResourceInformation resource, FintRelation fintRelation) {
        String packageName = fintRelation.getPackageName();

        if (shouldAddRelation(packageName)) {
            resourceLinkIdMap.get(resource.name()).put(
                    fintRelation.getName().toLowerCase(),
                    convertIdFieldsToLowercase(fintRelation)
            );
        }
    }

    private boolean shouldAddRelation(String packageName) {
        return resourceContext.notAbstract(packageName) && resourceContext.notFintReference(packageName);
    }

    private Set<String> convertIdFieldsToLowercase(FintRelation fintRelation) {
        return resourceContext.getRelation(fintRelation.getPackageName()).idFields().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

}
