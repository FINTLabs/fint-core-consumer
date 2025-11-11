package no.fintlabs.cache;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.cache.config.CacheConfig;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.kafka.KafkaHeader;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static no.fintlabs.consumer.kafka.KafkaConstants.TOPIC_RETENTION_TIME;

@Slf4j
@Configuration
public class CacheService {

    private final ResourceContext resourceContext;
    private final CacheContainer cacheContainer;
    private final Map<String, byte[]> retentionTimeMap = new ConcurrentHashMap<>();
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

    public void updateRetentionTime(String resource, Header header) {
        if (header != null) {
            byte[] currentRetentionTimeValue = header.value();
            if (!Arrays.equals(retentionTimeMap.get(resource), currentRetentionTimeValue)) {
                retentionTimeMap.put(resource, currentRetentionTimeValue);
                long retensionTime = KafkaHeader.getLong(header);
                log.info("Updating {} cache retention time to {} ms", resource, retensionTime);
                getCache(resource).setRetentionPeriodInMs(retensionTime);
            }
        } else {
            log.debug("{} header is null, skipping update of retention time", TOPIC_RETENTION_TIME);
        }
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
