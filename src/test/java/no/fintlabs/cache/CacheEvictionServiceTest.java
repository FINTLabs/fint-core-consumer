package no.fintlabs.cache;

import no.fint.model.resource.FintResource;
import no.fint.model.resource.utdanning.elev.ElevResource;
import no.fintlabs.adapter.models.event.ResourceEvictionPayload;
import no.fintlabs.cache.Cache;
import no.fintlabs.cache.CacheEvictionService;
import no.fintlabs.cache.CacheService;
import no.fintlabs.cache.packing.PackingTypes;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CacheEvictionServiceTest {

    @Mock
    private TaskScheduler scheduler;

    @Mock
    private CacheService cacheService;

    @Mock
    private ResourceContext resourceContext;

    @Mock
    private ConsumerConfiguration configuration;

    @InjectMocks
    private CacheEvictionService cacheEvictionService;

    @Mock
    private Cache<FintResource> cache;

    private final String resourceName = "elev";

    @BeforeEach
    public void setUp() {
        when(configuration.getDomain()).thenReturn("utdanning");
        when(configuration.getPackageName()).thenReturn("elev");

        Set<String> resourceNames = Collections.singleton(resourceName);
        when(resourceContext.getResourceNames()).thenReturn(resourceNames);

        when(cacheService.getCache(anyString())).thenReturn(cache);

        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            return null;
        }).when(scheduler).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    public void testTriggerEvictionSchedulesTaskAndEvictsCache() {
        ResourceEvictionPayload payload = new ResourceEvictionPayload("utdanning", "elev", "elev", "fintlabs.no");
        cacheEvictionService.triggerEviction(payload);

        ArgumentCaptor<Instant> instantCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(scheduler).schedule(any(Runnable.class), instantCaptor.capture());
        Instant scheduledTime = instantCaptor.getValue();

        Instant now = Instant.now();
        Instant expectedTime = now.plus(Duration.ofMinutes(10));

        long secondsDifference = Duration.between(expectedTime, scheduledTime).abs().getSeconds();
        assertTrue(secondsDifference < 1, "Scheduled time should be roughly 10 minutes in the future");

        verify(cacheService).getCache("elev");
        verify(cache).evictOldCacheObjects();
    }

}
