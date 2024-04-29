package no.fintlabs.consumer.resource;

import no.fint.model.FintResource;
import no.fintlabs.cache.Cache;
import no.fintlabs.cache.CacheContainer;
import no.fintlabs.cache.CacheManager;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan("no.fintlabs.cache")
public class CacheService {

    private final ReflectionService reflectionService;
    private final CacheContainer cacheContainer;

    public CacheService(ReflectionService reflectionService, ConsumerConfiguration configuration, CacheManager cacheManager) {
        this.reflectionService = reflectionService;
        this.cacheContainer = createCacheContainer(configuration, cacheManager);
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