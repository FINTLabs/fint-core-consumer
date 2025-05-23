package no.fintlabs.consumer.resource;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ResourceMetrics {

    private final MeterRegistry meterRegistry;
    private final CacheService cacheService;
    private final ResourceContext resourceContext;
    private final ConsumerConfiguration configuration;

    @PostConstruct
    private void init() {
        resourceContext.getResources().forEach(resource -> registerCacheSize(resource.name()));
    }

    private void registerCacheSize(String resourceName) {
        Gauge.builder("core.cache.size", () -> cacheService.getCache(resourceName).size())
                .tag("resource", resourceName)
                .tag("org", configuration.getOrgId())
                .description("Number of entries in the cache for a given resource")
                .register(meterRegistry);
    }

}
