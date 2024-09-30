package no.fintlabs.reflection;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintModelObject;
import no.fint.model.FintRelation;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
@Getter
@Configuration
@RequiredArgsConstructor
public class ResourceContext {

    private final ResourceContextCache resourceContextCache;

    public Set<String> getResourceNames() {
        return resourceContextCache.resourceToResourceInformationMap.keySet();
    }

    public Collection<FintResourceInformation> getResources() {
        return resourceContextCache.resourceToResourceInformationMap.values();
    }

    public FintResourceInformation getResource(String resourceName) {
        return resourceContextCache.resourceToResourceInformationMap.get(resourceName.toLowerCase());
    }

    public FintRelationInformation getRelation(String packageName) {
        return resourceContextCache.packageToRelationInformationMap.get(packageName.toLowerCase());
    }

    public boolean resourceHasIdField(String resourceName, String idField) {
        return resourceContextCache.resourceToResourceInformationMap.get(resourceName.toLowerCase()).idFieldNames().contains(idField.toLowerCase());
    }

    public boolean resourceIsWriteable(String resourceName) {
        return resourceContextCache.resourceToResourceInformationMap.get(resourceName.toLowerCase()).isWriteable();
    }

}
