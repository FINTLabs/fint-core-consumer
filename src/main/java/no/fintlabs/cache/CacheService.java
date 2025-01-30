package no.fintlabs.cache;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Configuration;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Configuration
public class CacheService {

    private final ResourceContext resourceContext;
    private final CacheContainer cacheContainer;
    private final Map<String, byte[]> retentionTimeMap = new ConcurrentHashMap<>();

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

    public void updateRetentionTime(String resource, Header header) {
        if (header != null) {
            byte[] currentRetentionTimeValue = header.value();
            if (!Arrays.equals(retentionTimeMap.get(resource), currentRetentionTimeValue)) {
                retentionTimeMap.put(resource, currentRetentionTimeValue);
                long retensionTime = convertRetensionTime(header.value());
                log.debug("Updating retention time for resource: {} to {}-MS", resource, retensionTime);
                getCache(resource).setRetentionPeriodInMs(retensionTime);
            }
        } else {
            log.debug("Header is null");
        }
    }

    private long convertRetensionTime(byte[] value) {
        return ByteBuffer.allocate(8)
                .put(value)
                .flip()
                .getLong();
    }

    private CacheContainer createCacheContainer(ConsumerConfiguration configuration, CacheManager cacheManager) {
        CacheContainer cacheContainer = new CacheContainer(configuration, cacheManager);
        resourceContext.getResourceNames().forEach(resourceName -> cacheContainer.initializeCache(resourceName.toLowerCase()));
        return cacheContainer;
    }

}
