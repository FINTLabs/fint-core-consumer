package no.fintlabs.cache;

import no.fint.model.resource.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class CacheService {

    private final ResourceContext resourceContext;
    private final CacheContainer cacheContainer;

    public CacheService(ResourceContext resourceContext, ConsumerConfiguration configuration, CacheManager cacheManager) {
        this.resourceContext = resourceContext;
        this.cacheContainer = createCacheContainer(configuration, cacheManager);
    }

    public int getSizeByResource(String resource) {
        return cacheContainer.getCache(resource).size();
    }

    public Map<String, Cache<FintResource>> getResourceCaches() {
        return cacheContainer.getResourceCache();
    }

    public Cache<FintResource> getCache(String resource) {
        return cacheContainer.getCache(resource);
    }

    private CacheContainer createCacheContainer(ConsumerConfiguration configuration, CacheManager cacheManager) {
        CacheContainer cacheContainer = new CacheContainer(configuration, cacheManager);
        resourceContext.getResourceNames().forEach(resourceName -> cacheContainer.initializeCache(resourceName.toLowerCase()));
        return cacheContainer;
    }

}
