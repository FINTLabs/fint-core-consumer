package no.fintlabs.consumer.links;

import no.fint.model.FintMultiplicity;
import no.fint.model.FintRelation;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.FintResourceInformation;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class LinkRelations {

    private final Map<String, Map<String, String>> resourceRelationLinksMap = new HashMap<>();
    private final Map<String, Set<String>> resourceRelationRequiredLinkMap = new HashMap<>();
    private final Map<String, Map<String, String>> lowercaseRelationNameToTrueRelationNameMap = new HashMap<>();
    private final Map<String, Set<String>> resourceToRelatioNamesMap = new HashMap<>();
    private final ConsumerConfiguration configuration;

    public LinkRelations(ResourceContext resourceContext, ConsumerConfiguration configuration) {
        this.configuration = configuration;
        setResourceRelationLinksMap(resourceContext);
        setResourceRelationRequiredLinkMap(resourceContext);
        setResourceToRelationNamesMap(resourceContext);
        setLowercaseRelationNameToTrueRelationNameMap(resourceContext);
    }

    public String getRelationName(String resourceName, String relationName) {
        return lowercaseRelationNameToTrueRelationNameMap.get(resourceName)
                .get(relationName);
    }

    public boolean relationNameExists(String resourceName, String relationName) {
        return resourceToRelatioNamesMap.get(resourceName).contains(relationName.toLowerCase());
    }

    private void setResourceToRelationNamesMap(ResourceContext resourceContext) {
        resourceContext.getResources().forEach(resource ->
                resource.relations().forEach(relation ->
                        resourceToRelatioNamesMap.computeIfAbsent(resource.name(), k -> new HashSet<>())
                                .add(relation.getName().toLowerCase())
                )
        );
    }

    private void setLowercaseRelationNameToTrueRelationNameMap(ResourceContext resourceContext) {
        resourceContext.getResources().forEach(resource -> {
                    lowercaseRelationNameToTrueRelationNameMap.put(resource.name(), new HashMap<>());
                    resource.relations().forEach(relation -> {
                        lowercaseRelationNameToTrueRelationNameMap.get(resource.name()).put(
                                relation.getName().toLowerCase(),
                                relation.getName()
                        );
                    });
                }
        );
    }


    private void setResourceRelationRequiredLinkMap(ResourceContext resourceContext) {
        resourceContext.getResources().forEach(resource -> resourceRelationRequiredLinkMap.put(
                resource.name(),
                getRequiredRelationNames(resource)
        ));
    }

    private Set<String> getRequiredRelationNames(FintResourceInformation resource) {
        return resource.relations().stream()
                .filter(relation -> relation.getMultiplicity().equals(FintMultiplicity.ONE_TO_ONE))
                .map(FintRelation::getName)
                .collect(Collectors.toSet());
    }

    private void setResourceRelationLinksMap(ResourceContext resourceContext) {
        resourceContext.getResources().forEach(resource ->
                resourceRelationLinksMap.put(
                        resource.name(),
                        createRelationToUriMap(resource)
                )
        );
    }

    private Map<String, String> createRelationToUriMap(FintResourceInformation resource) {
        return resource.relations().stream()
                .collect(Collectors.toMap(
                        relation -> relation.getName().toLowerCase(),
                        this::createUri
                ));
    }

    private String createUri(FintRelation relation) {
        String[] packageSplit = relation.getPackageName().split("\\.");
        if (isAPartOfCommonLibrary(packageSplit)) {
            return "%s/%s/%s".formatted(configuration.getDomain(), configuration.getPackageName(), relation.getName().toLowerCase());
        } else {
            return relation.getPackageName()
                    .replaceFirst("no.fint.model.", "")
                    .replace(".", "/")
                    .toLowerCase();
        }
    }

    private boolean isAPartOfCommonLibrary(String[] packageSplit) {
        return packageSplit.length == 5;
    }

    public Set<String> getRequiredRelationNamesOfResource(String resourceName) {
        return resourceRelationRequiredLinkMap.get(resourceName);
    }

    public String getRelationUri(String resourceName, String relationName) {
        return resourceRelationLinksMap.get(resourceName).get(relationName.toLowerCase());
    }
}
