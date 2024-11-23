package no.fintlabs.consumer.resource.context;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.resource.context.model.FintResourceInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@Getter
@Configuration
public class ResourceContext {

    private final ResourceContextCache contextCache;
    private final Set<String> writeableResources;

    public ResourceContext(@Value("${fint.consumer.writeable:test}") String writeable, ResourceContextCache contextCache) {
        this.writeableResources = new HashSet<>(Arrays.asList(writeable.split(",")));
        this.contextCache = contextCache;
    }

    public boolean relationContainsIdField(String resourceName, String relationName, String idField) {
        return contextCache.resourceMap.get(resourceName.toLowerCase()).relations()
                .get(relationName.toLowerCase()).idFields()
                .contains(idField);
    }

    public boolean isNotFintReference(String resourceName, String relationName) {
        return contextCache.resourceMap.get(resourceName.toLowerCase())
                .referenceNames()
                .contains(relationName.toLowerCase());
    }

    public boolean isNotFintAbstractObject(String resourceName, String relationName) {
        return contextCache.resourceMap.get(resourceName.toLowerCase())
                .abstractRelations()
                .contains(relationName.toLowerCase());
    }

    public Set<String> getResourceNames() {
        return contextCache.resourceMap.keySet();
    }

    public Collection<FintResourceInformation> getResources() {
        return contextCache.resourceMap.values();
    }

    public FintResourceInformation getResource(String resourceName) {
        return contextCache.resourceMap.get(resourceName.toLowerCase());
    }

    public boolean resourceHasIdField(String resourceName, String idField) {
        return contextCache.resourceMap.get(resourceName.toLowerCase())
                .idFieldNames()
                .contains(idField.toLowerCase());
    }

    public boolean resourceIsWriteable(String resourceName) {
        return contextCache.resourceMap.get(resourceName.toLowerCase()).isWriteable()
                || writeableResources.contains(resourceName.toLowerCase());
    }

    public String getRelationUri(String resourceName, String relationName) {
        return contextCache.resourceMap.get(resourceName.toLowerCase())
                .relations()
                .get(relationName.toLowerCase())
                .uri();
    }
}
