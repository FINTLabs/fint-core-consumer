package no.fintlabs.consumer.links;

import no.fint.model.FintMultiplicity;
import no.fint.model.FintRelation;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class LinkRelations {

    private final Map<String, Map<String, String>> resourceRelationLinksMap = new HashMap<>();
    private final Map<String, Set<String>> resourceRelationRequiredLinkMap = new HashMap<>();

    public LinkRelations(ResourceContext resourceContext) {
        setResourceRelationLinksMap(resourceContext);
        setResourceRelationRequiredLinkMap(resourceContext);
    }

    private void setResourceRelationRequiredLinkMap(ResourceContext resourceContext) {
        resourceContext.getFintResourceInformationMap().forEach((resourceName, resourceInformation) -> {
            resourceRelationRequiredLinkMap.put(
                    resourceName,
                    resourceInformation.relations().stream()
                            .filter(relation -> relation.getMultiplicity().equals(FintMultiplicity.ONE_TO_ONE))
                            .map(FintRelation::getName)
                            .collect(Collectors.toSet())
            );
        });
    }

    // Example of a relation PackageName = no.fint.model.utdanning.vurdering.elevfravar
    private void setResourceRelationLinksMap(ResourceContext resourceContext) {
        resourceContext.getFintResourceInformationMap().forEach((resourceName, resourceInformation) ->
                resourceRelationLinksMap.put(
                        resourceName,
                        resourceInformation.relations().stream()
                                .collect(Collectors.toMap(
                                        relation -> relation.getName().toLowerCase(),
                                        relation -> relation.getPackageName()
                                                .replaceFirst("no.fint.model.", "")
                                                .replace(".", "/")
                                                .toLowerCase()
                                ))
                )
        );
    }

    public Set<String> getRequiredRelationNamesOfResource(String resourceName) {
        return resourceRelationRequiredLinkMap.get(resourceName);
    }

    public String getRelationUri(String resourceName, String relationName) {
        return resourceRelationLinksMap.get(resourceName).get(relationName);
    }
}
