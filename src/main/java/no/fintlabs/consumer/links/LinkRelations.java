package no.fintlabs.consumer.links;

import no.fintlabs.reflection.ReflectionService;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class LinkRelations {

    private final Map<String, Map<String, String>> resourceRelationLinks = new HashMap<>();

    public LinkRelations(ReflectionService reflectionService) {
        setResourceRelationLinks(reflectionService);
    }

    private void setResourceRelationLinks(ReflectionService reflectionService) {
        reflectionService.getResources().forEach((resource, resourceInformation) ->
                resourceRelationLinks.put(
                        resource,
                        resourceInformation.relations().stream().collect(Collectors.toMap(
                                fintrelation -> fintrelation.getName().toLowerCase(),
                                fintRelation -> fintRelation.getPackageName().replaceFirst("no.fint.model", "").replace(".", "/").toLowerCase()
                        ))
                )
        );
    }

    public String getRelationUri(String resourceName, String relationName) {
        return resourceRelationLinks.get(resourceName).get(relationName);
    }
}
