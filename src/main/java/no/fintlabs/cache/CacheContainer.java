package no.fintlabs.cache;

import no.fint.model.FintResource;
import no.fintlabs.cache.packing.PackingTypes;
import no.fintlabs.consumer.config.ConsumerConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheContainer {

    private final ConsumerConfiguration consumerConfig;
    private final Map<String, Cache<FintResource>> resourceCache = new ConcurrentHashMap<>();
    private final CacheManager cacheManager;

    public CacheContainer(ConsumerConfiguration consumerConfig, CacheManager cacheManager) {
        this.consumerConfig = consumerConfig;
        this.cacheManager = cacheManager;
    }

    public Cache<FintResource> getCache(String resource) {
        return resourceCache.get(resource);
    }

    public void initializeCache(String resource) {
        resourceCache.putIfAbsent(resource, cacheManager.create(PackingTypes.POJO, consumerConfig.getOrgId(), resource));
    }

}
