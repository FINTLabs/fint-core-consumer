package no.fintlabs.consumer;

import no.fint.model.resource.FintResource;
import no.fintlabs.cache.Cache;
import no.fintlabs.cache.CacheContainer;
import no.fintlabs.cache.CacheManager;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ComponentScan("no.fintlabs.cache")
public class CacheService {

    private final ReflectionService reflectionService;
    private final CacheContainer cacheContainer;

    public CacheService(ReflectionService reflectionService, ConsumerConfiguration configuration, CacheManager cacheManager) {
        this.reflectionService = reflectionService;
        this.cacheContainer = createCacheContainer(configuration, cacheManager);
    }

    public Map<String, Cache<FintResource>> getResourceCaches() {
        return cacheContainer.getResourceCache();
    }

    public Cache<FintResource> getCache(String resource) {
        return cacheContainer.getCache(resource);
    }

    private CacheContainer createCacheContainer(ConsumerConfiguration configuration, CacheManager cacheManager) {
        CacheContainer cacheContainer = new CacheContainer(configuration, cacheManager);

        reflectionService.getResources().forEach((resource, idField) -> cacheContainer.initializeCache(resource.toLowerCase()));

        return cacheContainer;
    }

}
