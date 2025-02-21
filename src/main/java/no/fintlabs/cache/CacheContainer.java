package no.fintlabs.cache;

import lombok.Getter;
import no.fint.model.resource.FintResource;
import no.fintlabs.cache.packing.PackingTypes;
import no.fintlabs.consumer.config.ConsumerConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheContainer {

    @Getter
    private final Map<String, Cache<FintResource>> resourceCache = new ConcurrentHashMap<>();

    private final ConsumerConfiguration consumerConfig;
    private final CacheManager cacheManager;

    public CacheContainer(ConsumerConfiguration consumerConfig, CacheManager cacheManager) {
        this.consumerConfig = consumerConfig;
        this.cacheManager = cacheManager;
    }

    public Cache<FintResource> getCache(String resource) {
        return resourceCache.get(resource);
    }

    public void initializeCache(String resource, long retentionTime) {
        resourceCache.putIfAbsent(resource, cacheManager.create(PackingTypes.POJO, consumerConfig.getOrgId(), resource));
        resourceCache.get(resource).setRetentionPeriodInMs(retentionTime);
    }

}
