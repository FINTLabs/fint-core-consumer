package no.fintlabs.consumer.links;

import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class LinkRelations {

    private final Map<String, Map<String, String>> resourceRelationLinks = new HashMap<>();

    public LinkRelations(ResourceContext resourceContext) {
        setResourceRelationLinks(resourceContext);
    }

    private void setResourceRelationLinks(ResourceContext resourceContext) {
        resourceContext.getFintResourceInformationMap().forEach((resourceName, resourceInformation) ->
                resourceRelationLinks.put(
                        resourceName,
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
