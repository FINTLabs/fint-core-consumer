package no.fintlabs.cache;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.adapter.models.event.ResourceEvictionPayload;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheEvictionService {

    private static final Long MINUTES_TO_WAIT = 10L;

    private final TaskScheduler scheduler;
    private final CacheService cacheService;
    private final ResourceContext resourceContext;
    private final ConsumerConfiguration configuration;

    public void triggerEviction(ResourceEvictionPayload resourceEvictionPayload) {
        if (payloadBelongsToThisConsumer(resourceEvictionPayload) && requestIsWithinTenMinutes(resourceEvictionPayload)) {
            log.info("Eviction request triggered. Resource: {}", resourceEvictionPayload.getResource());
            scheduler.schedule(
                    () -> processEviction(resourceEvictionPayload),
                    Instant.now().plus(Duration.ofMinutes(MINUTES_TO_WAIT))
            );
        }
    }

    private void processEviction(ResourceEvictionPayload resourceEvictionPayload) {
        log.info("Eviction ongoing. Resource: {}", resourceEvictionPayload.getResource());
        cacheService.getCache(resourceEvictionPayload.getResource().toLowerCase()).evictOldCacheObjects();
    }

    private boolean payloadBelongsToThisConsumer(ResourceEvictionPayload resourceEvictionPayload) {
        return configuration.getOrgId().equalsIgnoreCase(resourceEvictionPayload.getOrg().replace("-", "."))
                && configuration.getDomain().equalsIgnoreCase(resourceEvictionPayload.getDomain())
                && configuration.getPackageName().equalsIgnoreCase(resourceEvictionPayload.getPkg())
                && resourceContext.getResourceNames().contains(resourceEvictionPayload.getResource().toLowerCase());
    }

    private boolean requestIsWithinTenMinutes(ResourceEvictionPayload payload) {
        Instant payloadTime = Instant.ofEpochSecond(payload.getUnixTimestamp());
        Instant now = Instant.now();

        if (payloadTime.isAfter(now)) return false;

        Duration elapsedTime = Duration.between(payloadTime, now);
        return elapsedTime.compareTo(Duration.ofMinutes(10)) <= 0;
    }

}
