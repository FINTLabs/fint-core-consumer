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
import no.fintlabs.consumer.kafka.entity.KafkaEntity;
import no.fintlabs.consumer.kafka.event.EventProducer;
import no.fintlabs.consumer.resource.event.EventService;
import no.fintlabs.model.resource.FintResources;
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
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.UUID;

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
    private EventService eventService;

    @Autowired
    private CacheService cacheService;

    @MockitoBean
    private EventProducer eventProducer;

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
        FintResource result = resourceController.getResourceById(RESOURCE_NAME, "systemid", "5");
        assertEquals("5", result.getIdentifikators().get("systemId").getIdentifikatorverdi());
    }

    @Test
    void testGetResourceByIdFailure_WhenIdDoesntMatch() {
        assertThrows(ResponseStatusException.class, () -> resourceController.getResourceById(RESOURCE_NAME, "systemid", "53232"));
    }

    @Test
    void testGetLastUpdated() {
        Map<String, Long> lastUpdated = resourceController.getLastUpdated(RESOURCE_NAME);
        assertInstanceOf(Long.class, lastUpdated.get("lastUpdated"));
    }

    @Test
    void testGetResourceCacheSize() {
        Map<String, Integer> resourceCacheSize = resourceController.getResourceCacheSize(RESOURCE_NAME);
        assertEquals(100, resourceCacheSize.get("size"));
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
    void notFound_WhenEventIsNotPresent() {
        assertEquals(HttpStatus.NOT_FOUND, resourceController.getStatus(WRITEABLE_RESOURCE_NAME, UUID.randomUUID().toString()).getStatusCode());
    }

    @Test
    void testStatusReturnsAccepted_WhenRequestIsPresent() {
        String corrId = UUID.randomUUID().toString();
        eventService.registerRequest(corrId);
        assertEquals(HttpStatus.ACCEPTED, resourceController.getStatus(WRITEABLE_RESOURCE_NAME, corrId).getStatusCode());
    }

    @Test
    void internalServerError_WhenEventHasFailed() {
        String corrId = "123";
        FintResource eksamensgruppeResource = EksamensgruppeResource(123123);

        eventService.registerRequest(corrId);
        eventService.registerResponse(corrId, createResponseFintEvent(eksamensgruppeResource, true, false, false, OperationType.CREATE));

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, resourceController.getStatus(WRITEABLE_RESOURCE_NAME, corrId).getStatusCode());
    }

    @Test
    void okStatus_WhenEventIsValidated() {
        String corrId = UUID.randomUUID().toString();
        ResponseFintEvent event = ResponseFintEvent.builder().operationType(OperationType.VALIDATE).build();

        eventService.registerResponse(corrId, event);

        assertEquals(HttpStatus.OK, resourceController.getStatus(WRITEABLE_RESOURCE_NAME, corrId).getStatusCode());
    }

    @Test
    void noContentResponse_WhenEventIsDeleting() {
        String corrId = UUID.randomUUID().toString();
        ResponseFintEvent event = ResponseFintEvent.builder().operationType(OperationType.DELETE).build();

        eventService.registerResponse(corrId, event);

        assertEquals(HttpStatus.NO_CONTENT, resourceController.getStatus(WRITEABLE_RESOURCE_NAME, corrId).getStatusCode());
    }

    @Test
    void conflictResponseSuccess() {
        String corrId = UUID.randomUUID().toString();
        String resourceId = "123";
        FintResource elevResource = createElevResource(resourceId);
        ResponseFintEvent event = ResponseFintEvent.builder().corrId(corrId).conflicted(true).value(SyncPageEntry.of(resourceId, elevResource)).operationType(OperationType.CREATE).build();

        eventService.registerResponse(corrId, event);

        ResponseEntity<Object> response = resourceController.getStatus(WRITEABLE_RESOURCE_NAME, corrId);
        assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
        assertInstanceOf(ElevResource.class, response.getBody());

        elevResource = (ElevResource) response.getBody();
        assertEquals("https://test.felleskomponent.no/utdanning/elev/elev/systemid/%s".formatted(resourceId), elevResource.getSelfLinks().getFirst().getHref());
    }

    @Test
    void badRequest_WhenEventIsRejected() {
        String corrId = "123";
        ResponseFintEvent event = ResponseFintEvent.builder().corrId(corrId).operationType(OperationType.CREATE).rejected(true).build();

        eventService.registerRequest(corrId);
        eventService.registerResponse(corrId, event);

        assertEquals(HttpStatus.BAD_REQUEST, resourceController.getStatus(WRITEABLE_RESOURCE_NAME, corrId).getStatusCode());
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
        assertThrows(IdentificatorNotFoundException.class, () -> resourceController.putResource(WRITEABLE_RESOURCE_NAME, "NotAnIdField", "123", EksamensgruppeResource(402)));
    }

    private KafkaEntity createEntityConsumerRecord(String key, FintResource resource) {
        return new KafkaEntity(key, RESOURCE_NAME, resource, System.currentTimeMillis(), SyncType.FULL, "test-corr-id", 100L);
    }

    private RequestFintEvent createRequestFintEvent(String corrId) {
        return RequestFintEvent.builder().resourceName(WRITEABLE_RESOURCE_NAME).corrId(corrId).build();
    }

    private ResponseFintEvent createResponseFintEvent(FintResource fintResource, boolean failed, boolean isRejected, boolean isConflicted, OperationType operationType) {
        return ResponseFintEvent.builder().value(SyncPageEntry.of("321", fintResource)).failed(failed).rejected(isRejected).conflicted(isConflicted).operationType(operationType).build();
    }

    private FintResource EksamensgruppeResource(int i) {
        EksamensgruppeResource eksamensgruppeResource = new EksamensgruppeResource();
        eksamensgruppeResource.setSystemId(new Identifikator() {{
            setIdentifikatorverdi(String.valueOf(i));
        }});
        return eksamensgruppeResource;
    }

    private ElevResource createElevResource(String id) {
        ElevResource elevResource = new ElevResource();
        elevResource.setSystemId(new Identifikator() {{
            setIdentifikatorverdi(id);
        }});
        return elevResource;
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
