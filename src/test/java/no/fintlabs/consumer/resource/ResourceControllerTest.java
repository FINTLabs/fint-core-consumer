package no.fintlabs.consumer.resource;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.ElevResource;
import no.fint.model.resource.utdanning.elev.ElevforholdResource;
import no.fint.model.resource.utdanning.vurdering.EksamensgruppeResource;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import no.fintlabs.adapter.models.sync.SyncPageEntry;
import no.fintlabs.adapter.operation.OperationType;
import no.fintlabs.consumer.exception.resource.IdentificatorNotFoundException;
import no.fintlabs.consumer.exception.resource.ResourceNotWriteableException;
import no.fintlabs.consumer.kafka.event.EventProducer;
import no.fintlabs.consumer.resource.event.EventService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
public class ResourceControllerTest {

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private ResourceController resourceController;

    @Autowired
    private EventService eventService;

    @MockBean
    private EventProducer eventProducer;

    private static final String RESOURCENAME = "elevforhold";
    private static final String WRITEABLE_RESOURCENAME = "elev";

    @BeforeEach
    public void setUp() {
        for (int i = 0; i < 100; i++) {
            resourceService.addResourceToCache(RESOURCENAME, String.valueOf(i), createElevforholdResource(i));
        }
    }

    @Test
    void testGetResourceSuccess() {
        FintResources resources = resourceController.getResource(RESOURCENAME, 0, 0, 0, null);
        assertEquals(resources.getTotalItems(), 100);
        assertEquals(resources.getContent().size(), 100);
        assertEquals(resources.getSize(), 100);
    }

    @Test
    void testGetResourceSuccess_WhenSizeIsSet() {
        FintResources resources = resourceController.getResource(RESOURCENAME, 5, 0, 0, null);
        assertEquals(resources.getTotalItems(), 100);
        assertEquals(resources.getContent().size(), 5);
        assertEquals(resources.getSize(), 5);
    }

    @Test
    void testGetResourceSuccess_WhenOffsetIsSet() {
        FintResources resources = resourceController.getResource(RESOURCENAME, 5, 10, 0, null);
        assertEquals(resources.getTotalItems(), 100);
        assertEquals(resources.getContent().size(), 5);
        assertEquals(resources.getSize(), 5);
        assertEquals(resources.getOffset(), 10);
    }

    @Test
    void testGetResourceByIdSuccess() {
        ResponseEntity<FintResource> response = resourceController.getResourceById(RESOURCENAME, "systemid", "5");
        FintResource fintResource = response.getBody();

        assertEquals(response.getStatusCode().value(), 200);
        assertEquals(fintResource.getIdentifikators().get("systemId").getIdentifikatorverdi(), "5");
    }

    @Test
    void testGetResourceByIdFailure_WhenIdDoesntMatch() {
        ResponseEntity<FintResource> response = resourceController.getResourceById(RESOURCENAME, "systemid", "53232");
        assertEquals(response.getStatusCode().value(), 404);
    }

    @Test
    void testGetLastUpdated() {
        Map<String, Long> lastUpdated = resourceController.getLastUpdated(RESOURCENAME);
        assertInstanceOf(Long.class, lastUpdated.get("lastUpdated"));
    }

    @Test
    void testGetResourceCacheSize() {
        Map<String, Integer> resourceCacheSize = resourceController.getResourceCacheSize(RESOURCENAME);
        assertEquals(resourceCacheSize.get("size"), 100);
    }

    @Test
    void testPostResourceThrowsException_WhenResourceIsNotWriteable() {
        assertThrows(ResourceNotWriteableException.class, () -> resourceController.postResource(RESOURCENAME, createElevforholdResource(101), false));
    }

    @Test
    void testPostResourceSuccess() {
        String corrId = "123";

        when(eventProducer.sendEvent(any(String.class), any(Object.class), any(OperationType.class)))
                .thenReturn(createRequestFintEvent(WRITEABLE_RESOURCENAME, corrId));

        ResponseEntity<?> responseEntity = resourceController.postResource(WRITEABLE_RESOURCENAME, createElevforholdResource(0), false);
        String location = responseEntity.getHeaders().get("Location").getFirst();

        assertEquals(HttpStatus.ACCEPTED, responseEntity.getStatusCode());
        assertEquals(location, "https://test.felleskomponent.no/utdanning/elev/%s/status/123".formatted(WRITEABLE_RESOURCENAME));
    }

    @Test
    void notFound_WhenEventIsNotPresent() {
        assertEquals(
                HttpStatus.NOT_FOUND,
                resourceController.getStatus(WRITEABLE_RESOURCENAME, UUID.randomUUID().toString()).getStatusCode()
        );
    }

    @Test
    void testStatusReturnsAccepted_WhenRequestIsPresent() {
        String corrId = UUID.randomUUID().toString();
        eventService.registerRequest(corrId);
        assertEquals(HttpStatus.ACCEPTED, resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId).getStatusCode());
    }

    @Test
    void internalServerError_WhenEventHasFailed() {
        String corrId = "123";
        FintResource eksamensgruppeResource = EksamensgruppeResource(123123);

        eventService.registerRequest(corrId);
        eventService.registerResponse(corrId, createResponseFintEvent(eksamensgruppeResource, true, false, false, OperationType.CREATE));

        assertEquals(
                HttpStatus.INTERNAL_SERVER_ERROR,
                resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId).getStatusCode()
        );
    }

    @Test
    void okStatus_WhenEventIsValidated() {
        String corrId = UUID.randomUUID().toString();
        ResponseFintEvent event = ResponseFintEvent.builder().operationType(OperationType.VALIDATE).build();

        eventService.registerResponse(corrId, event);

        assertEquals(
                HttpStatus.OK,
                resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId).getStatusCode()
        );
    }

    @Test
    void noContentResponse_WhenEventIsDeleting() {
        String corrId = UUID.randomUUID().toString();
        ResponseFintEvent event = ResponseFintEvent.builder().operationType(OperationType.DELETE).build();

        eventService.registerResponse(corrId, event);

        assertEquals(
                HttpStatus.NO_CONTENT,
                resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId).getStatusCode()
        );
    }

    @Test
    void conflictResponseSuccess() {
        String corrId = UUID.randomUUID().toString();
        String resourceId = "123";
        FintResource elevResource = createElevResource(resourceId);
        ResponseFintEvent event = ResponseFintEvent.builder()
                .corrId(corrId)
                .conflicted(true)
                .value(SyncPageEntry.of(resourceId, elevResource))
                .operationType(OperationType.CREATE)
                .build();

        eventService.registerResponse(corrId, event);

        ResponseEntity<Object> response = resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId);
        assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
        assertInstanceOf(ElevResource.class, response.getBody());

        elevResource = (ElevResource) response.getBody();
        assertEquals(
                "https://test.felleskomponent.no/utdanning/elev/elev/systemid/%s".formatted(resourceId),
                elevResource.getSelfLinks().getFirst().getHref()
        );
    }

    @Test
    void badRequest_WhenEventIsRejected() {
        String corrId = "123";
        ResponseFintEvent event = ResponseFintEvent.builder().corrId(corrId).operationType(OperationType.CREATE).rejected(true).build();

        eventService.registerRequest(corrId);
        eventService.registerResponse(corrId, event);

        assertEquals(
                HttpStatus.BAD_REQUEST,
                resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId).getStatusCode()
        );
    }

    @Test
    void testPutResourceSuccess() {
        String corrId = "123";
        when(eventProducer.sendEvent(any(String.class), any(Object.class), any(OperationType.class)))
                .thenReturn(createRequestFintEvent(WRITEABLE_RESOURCENAME, corrId));

        ResponseEntity<Void> voidResponseEntity = resourceController.putResource(WRITEABLE_RESOURCENAME, "systemid", "", EksamensgruppeResource(402));
        String location = voidResponseEntity.getHeaders().get("Location").getFirst();
        assertEquals(HttpStatus.ACCEPTED, voidResponseEntity.getStatusCode());
        assertEquals(
                "https://test.felleskomponent.no/utdanning/elev/%s/status/%s".formatted(WRITEABLE_RESOURCENAME, corrId),
                location
        );

    }

    @Test
    void testPutResourceFailure_WhenIdentifierFieldIsWrong() {
        assertThrows(IdentificatorNotFoundException.class, () ->
                resourceController.putResource(
                        WRITEABLE_RESOURCENAME, "NotAnIdField", "123", EksamensgruppeResource(402))
        );
    }

    private RequestFintEvent createRequestFintEvent(String resourceName, String corrId) {
        return RequestFintEvent.builder()
                .resourceName(resourceName)
                .corrId(corrId)
                .build();
    }

    private ResponseFintEvent createResponseFintEvent(FintResource fintResource, boolean failed, boolean isRejected, boolean isConflicted, OperationType operationType) {
        return ResponseFintEvent.builder()
                .value(SyncPageEntry.of("321", fintResource))
                .failed(failed)
                .rejected(isRejected)
                .conflicted(isConflicted)
                .operationType(operationType)
                .build();
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
