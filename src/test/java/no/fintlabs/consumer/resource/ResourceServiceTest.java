package no.fintlabs.consumer.resource;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.ElevResource;
import no.fintlabs.cache.CacheService;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ResourceServiceTest {

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private CacheService cacheService;

    private final String resourceName = "elev";

    @Test
    public void evictResource_WhenResourceHasExpired() {
        String idValue = "123";

        cacheService.getCache(resourceName).setRetentionPeriodInMs(1000L);
        resourceService.addResourceToCache(resourceName, UUID.randomUUID().toString(), createElevResource(idValue), null);

        cacheService.evictOldResources();
        assertEquals(1, cacheService.getCache(resourceName).size());

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        cacheService.evictOldResources();
        assertEquals(0, cacheService.getCache(resourceName).size());
    }

    @Test
    public void onlyEvictOldResources() {
        int retentionInDays = 7;

        cacheService.getCache(resourceName).setRetentionPeriodInMs(Duration.ofDays(retentionInDays).toMillis());
        for (int i = 0; i < 1000; i++) {
            resourceService.addResourceToCache(resourceName, String.valueOf(i), createElevResource(String.valueOf(i)), null);
        }

        cacheService.evictOldResources();
        assertEquals(1000, cacheService.getCache(resourceName).size());

        Long eightDaysAgoInMilliseconds = System.currentTimeMillis() - Duration.ofDays(8).toMillis();
        resourceService.addResourceToCache(resourceName, "5000", createElevResource("5000"), createEntiyRetentionHeader(eightDaysAgoInMilliseconds));

        assertEquals(1001, cacheService.getCache(resourceName).size());

        cacheService.evictOldResources();

        assertEquals(1000, cacheService.getCache(resourceName).size());
    }

    @Test
    public void mapResourceAndLinksSuccess() {
        ElevResource elevResource = createElevResource("123");
        elevResource.addElevforhold(Link.with("systemid/321"));

        FintResource fintResource = resourceService.mapResourceAndLinks("elev", elevResource);

        assertEquals(
                "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/321",
                fintResource.getLinks().get("elevforhold").getFirst().getHref()
        );
    }

    private Header createEntiyRetentionHeader(Long timeInMs) {
        return new RecordHeader("entity-retention-time", ByteBuffer.allocate(Long.BYTES).putLong(timeInMs).array());
    }

    private ElevResource createElevResource(String id) {
        return new ElevResource() {{
            setSystemId(new Identifikator() {{
                setIdentifikatorverdi(id);
            }});
        }};
    }

}
