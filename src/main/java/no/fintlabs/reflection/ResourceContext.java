package no.fintlabs.reflection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.Set;

@Slf4j
@Getter
@Configuration
public class ResourceContext {

    private final ResourceContextCache resourceContextCache;
    private final Set<String> writeableResources;

    public ResourceContext(@Value("${fint.consumer.writeable:[]}") String[] writeableResources, ResourceContextCache resourceContextCache) {
        log.info("RESOURCES: {}", writeableResources);
        this.resourceContextCache = resourceContextCache;
        this.writeableResources = Set.of(writeableResources);
    }

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
        return resourceContextCache.resourceToResourceInformationMap.get(resourceName.toLowerCase()).isWriteable() ||
                writeableResources.contains(resourceName.toLowerCase());
    }

}
