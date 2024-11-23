package no.fintlabs.consumer.resource;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ResourceMetrics {

    private final MeterRegistry meterRegistry;
    private final CacheService cacheService;
    private final ResourceContext resourceContext;

    @PostConstruct
    private void init() {
        resourceContext.getResources().forEach(resource -> {
            registerCacheSize(resource.name());
        });
    }

    private void registerCacheSize(String resourceName) {
        Gauge.builder("%s-cache-size".formatted(resourceName), () -> cacheService.getCache(resourceName).size())
                .register(meterRegistry);
    }

}
