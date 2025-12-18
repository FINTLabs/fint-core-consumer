package no.fintlabs.consumer.resource;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.ElevResource;
import no.fint.model.resource.utdanning.elev.ElevforholdResource;
import no.fint.model.resource.utdanning.vurdering.EksamensgruppeResource;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import no.fintlabs.adapter.models.sync.SyncPageEntry;
import no.fintlabs.adapter.models.sync.SyncType;
import no.fintlabs.adapter.operation.OperationType;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.exception.resource.IdentificatorNotFoundException;
import no.fintlabs.consumer.exception.resource.ResourceNotWriteableException;
import no.fintlabs.consumer.kafka.entity.EntityConsumerRecord;
import no.fintlabs.consumer.kafka.event.EventProducer;
import no.fintlabs.consumer.resource.event.EventService;
import no.fintlabs.model.resource.FintResources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static no.fintlabs.consumer.kafka.KafkaConstants.*;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("utdanning-elev")
@EmbeddedKafka
public class ResourceControllerTest {

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private ResourceController resourceController;

    @Autowired
    private EventStatusCache eventStatusCache;

    @Autowired
    private CacheService cacheService;

    @MockitoBean
    private RequestFintEventProducer eventProducer;

    private static final String RESOURCE_NAME = "elevforhold";
    private static final String WRITEABLE_RESOURCE_NAME = "elev";

    @BeforeEach
    public void setUp() {
        for (int i = 0; i < 100; i++) {
            resourceService.processEntityConsumerRecord(createEntityConsumerRecord(String.valueOf(i), createElevforholdResource(i)));
        }
    }

    @AfterEach
    public void tearDown() {
        // Clear cache between each test
        cacheService.getCache(RESOURCE_NAME).evictExpired(Long.MAX_VALUE);
    }

    @Test
    void oDataFilterSuccess() {
        ElevforholdResource elevforholdResource = new ElevforholdResource();
        elevforholdResource.setSystemId(new Identifikator() {{
            setIdentifikatorverdi("5002");
        }});
        elevforholdResource.setHovedskole(true);

        resourceService.processEntityConsumerRecord(createEntityConsumerRecord(UUID.randomUUID().toString(), elevforholdResource));

        FintResources resources = resourceController.getResource(RESOURCE_NAME, 0, 0, 0, "hovedskole eq 'true'");
        assertEquals(1, resources.getSize());
    }

    @Test
    void testGetResourceSuccess() {
        FintResources resources = resourceController.getResource(RESOURCE_NAME, 0, 0, 0, null);
        assertEquals(100, resources.getTotalItems());
        assertEquals(100, resources.getContent().size());
        assertEquals(100, resources.getSize());
    }

    @Test
    void testGetResourceSuccess_WhenSizeIsSet() {
        FintResources resources = resourceController.getResource(RESOURCE_NAME, 5, 0, 0, null);
        assertEquals(100, resources.getTotalItems());
        assertEquals(5, resources.getContent().size());
        assertEquals(5, resources.getSize());
    }

    @Test
    void testGetResourceSuccess_WhenOffsetIsSet() {
        FintResources resources = resourceController.getResource(RESOURCE_NAME, 5, 10, 0, null);
        assertEquals(100, resources.getTotalItems());
        assertEquals(5, resources.getContent().size());
        assertEquals(5, resources.getSize());
        assertEquals(10, resources.getOffset());
    }

    @Test
    void testGetResourceByIdSuccess() {
        ResponseEntity<FintResource> result = resourceController.getResourceById(RESOURCE_NAME, "systemid", "5");
        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertEquals("5", result.getBody().getIdentifikators().get("systemId").getIdentifikatorverdi());
    }

    @Test
    void shouldReturn404NotFound_WhenIdDoesntMatch() {
        assertEquals(
                HttpStatus.NOT_FOUND,
                resourceController.getResourceById(RESOURCENAME, "systemid", "53232").getStatusCode()
        );
    }

    @Test
    void testGetLastUpdated() {
        ResponseEntity<LastUpdatedResponse> response = resourceController.getLastUpdated(RESOURCE_NAME);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        assertNotNull(response.getBody());
        assertInstanceOf(Long.class, response.getBody().getLastUpdated());
    }

    @Test
    void testGetResourceCacheSize() {
        ResponseEntity<ResourceCacheSizeResponse> resourceCacheSize = resourceController.getResourceCacheSize(RESOURCE_NAME);
        assertEquals(resourceCacheSize.getBody().getSize(), 100);
    }

    @Test
    void testPostResourceThrowsException_WhenResourceIsNotWriteable() {
        assertThrows(ResourceNotWriteableException.class, () -> resourceController.postResource(RESOURCE_NAME, createElevforholdResource(101), false));
    }

    @Test
    void testPostResourceSuccess() {
        String corrId = "123";

        when(eventProducer.sendEvent(any(String.class), any(Object.class), any(OperationType.class))).thenReturn(createRequestFintEvent(corrId));

        ResponseEntity<?> responseEntity = resourceController.postResource(WRITEABLE_RESOURCE_NAME, createElevforholdResource(0), false);
        String location = responseEntity.getHeaders().get("Location").getFirst();

        assertEquals(HttpStatus.ACCEPTED, responseEntity.getStatusCode());
        assertEquals(location, "https://test.felleskomponent.no/utdanning/elev/%s/status/123".formatted(WRITEABLE_RESOURCE_NAME));
    }

    @Test
    void gone_WhenEventIsNotPresent() {
        assertEquals(
                HttpStatus.GONE,
                resourceController.getStatus(WRITEABLE_RESOURCE_NAME, UUID.randomUUID().toString()).getStatusCode()
        );
    }

    @Test
    void testPutResourceSuccess() {
        String corrId = "123";
        when(eventProducer.sendEvent(any(String.class), any(Object.class), any(OperationType.class))).thenReturn(createRequestFintEvent(corrId));

        ResponseEntity<Void> voidResponseEntity = resourceController.putResource(WRITEABLE_RESOURCE_NAME, "systemid", "", EksamensgruppeResource(402));
        String location = voidResponseEntity.getHeaders().get("Location").getFirst();
        assertEquals(HttpStatus.ACCEPTED, voidResponseEntity.getStatusCode());
        assertEquals("https://test.felleskomponent.no/utdanning/elev/%s/status/%s".formatted(WRITEABLE_RESOURCE_NAME, corrId), location);
    }

    @Test
    void testPutResourceFailure_WhenIdentifierFieldIsWrong() {
        assertThrows(IdentificatorNotFoundException.class, () ->
                resourceController.putResource(
                        WRITEABLE_RESOURCENAME, "NotAnIdField", "123", EksamensgruppeResource(402))
        );
    }

    private EntityConsumerRecord createEntityConsumerRecord(
            String resourceId,
            FintResource resource) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(SYNC_TYPE, new byte[]{(byte) SyncType.FULL.ordinal()}));
        headers.add(new RecordHeader(SYNC_CORRELATION_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
        headers.add(
                new RecordHeader(
                        SYNC_TOTAL_SIZE, ByteBuffer.allocate(Long.BYTES)
                        .putLong(100L)
                        .array()
                )
        );

        return new EntityConsumerRecord(
                RESOURCE_NAME,
                resource,
                new ConsumerRecord<>(
                        "test-topic",
                        0,
                        0,
                        System.currentTimeMillis(),
                        TimestampType.CREATE_TIME,
                        NULL_SIZE,
                        NULL_SIZE,
                        resourceId,
                        resource,
                        headers,
                        Optional.empty())
        );
    }

    private RequestFintEvent createRequestFintEvent(String resourceName, String corrId) {
        return RequestFintEvent.builder()
                .resourceName(resourceName)
                .corrId(corrId)
                .build();
    }

    private FintResource EksamensgruppeResource(int i) {
        EksamensgruppeResource eksamensgruppeResource = new EksamensgruppeResource();
        eksamensgruppeResource.setSystemId(new Identifikator() {{
            setIdentifikatorverdi(String.valueOf(i));
        }});
        return eksamensgruppeResource;
    }

    private FintResource createElevforholdResource(int index) {
        ElevforholdResource elevforholdResource = new ElevforholdResource();
        elevforholdResource.setSystemId(new Identifikator() {{
            setIdentifikatorverdi(String.valueOf(index));
        }});
        elevforholdResource.addElev(Link.with("systemid/test"));
        elevforholdResource.addSkole(Link.with("systemid/test"));
        return elevforholdResource;
    }

}
