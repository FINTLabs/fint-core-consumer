package no.fintlabs.consumer.resource;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ResourceIdentifierCache {

    private final ResourceContext resourceContext;
    private final Map<String, Map<String, String>> resourceToIdFieldMap = new HashMap<>();

    @PostConstruct
    private void init() {
        resourceContext.getResources().forEach(resource -> {
            Map<String, String> lowercaseIdToUppercaseId = new HashMap<>();
            resource.idFieldNames().forEach(idField ->
                    lowercaseIdToUppercaseId.put(idField.toLowerCase(), idField)
            );
            resourceToIdFieldMap.put(resource.name().toLowerCase(), lowercaseIdToUppercaseId);
        });
    }

    public String getIdField(String resourceName, String idField) {
        return resourceToIdFieldMap.get(resourceName.toLowerCase()).get(idField.toLowerCase());
    }

}
