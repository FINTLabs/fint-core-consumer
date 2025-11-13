package no.fintlabs.cache;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.cache.config.CacheConfig;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Configuration
public class CacheService {

    private final ResourceContext resourceContext;
    private final CacheContainer cacheContainer;
    private final Map<String, Long> retentionTimeMap = new ConcurrentHashMap<>();
    private final CacheConfig cacheConfig;

    public CacheService(ResourceContext resourceContext, ConsumerConfiguration configuration, CacheManager cacheManager, CacheConfig cacheConfig) {
        this.resourceContext = resourceContext;
        this.cacheConfig = cacheConfig;
        this.cacheContainer = createCacheContainer(configuration, cacheManager);
    }

    public int getSizeByResource(String resource) {
        return cacheContainer.getCache(resource).size();
    }

    public Map<String, Cache<FintResource>> getResourceCaches() {
        return cacheContainer.getResourceCache();
    }

    public Cache<FintResource> getCache(String resource) {
        return Objects.requireNonNull(
                cacheContainer.getCache(resource),
                () -> String.format("Cache for resource '%s' not initialised", resource)
        );
    }

    public void updateRetentionTime(String resource, @Nullable Long retentionTime) {
        if (retentionTime == null) {
            return;
        }

        Cache<FintResource> cache = getCache(resource);

        retentionTimeMap.compute(resource, (k, existingRetentionTime) -> {
            if (retentionTimeMismatch(existingRetentionTime, retentionTime)) {
                log.info("Updating cache '{}' retention: {} -> {} ms", resource, existingRetentionTime, retentionTime);
                cache.setRetentionPeriodInMs(retentionTime);
                return retentionTime;
            } else {
                return existingRetentionTime;
            }
        });
    }

    private boolean retentionTimeMismatch(Long existingRetentionTime, Long newRetentionTime) {
        return !Objects.equals(existingRetentionTime, newRetentionTime);
    }

    private CacheContainer createCacheContainer(ConsumerConfiguration configuration, CacheManager cacheManager) {
        CacheContainer cacheContainer = new CacheContainer(configuration, cacheManager);
        resourceContext.getResourceNames().forEach(resourceName -> {
                    log.info("Initializing cache: {} with retention time: {} ms", resourceName, cacheConfig.getRetention());
                    cacheContainer.initializeCache(resourceName, cacheConfig.getRetention());
                }
        );
        return cacheContainer;
    }

}
