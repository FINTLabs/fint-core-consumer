package no.fintlabs.consumer.resource;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.ElevforholdResource;
import no.fint.model.resource.utdanning.vurdering.EksamensgruppeResource;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import no.fintlabs.adapter.models.sync.SyncPageEntry;
import no.fintlabs.adapter.operation.OperationType;
import no.fintlabs.consumer.exception.event.EventFailedException;
import no.fintlabs.consumer.exception.event.EventNotFoundException;
import no.fintlabs.consumer.exception.event.EventRejectedException;
import no.fintlabs.consumer.exception.resource.IdentificatorNotFoundException;
import no.fintlabs.consumer.exception.resource.ResourceNotWriteableException;
import no.fintlabs.consumer.kafka.event.EventProducer;
import no.fintlabs.consumer.kafka.event.EventService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
@ComponentScan(
        excludeFilters = @ComponentScan.Filter(
                type = FilterType.REGEX,
                pattern = "no.fintlabs.kafka.*"
        )
)
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
        FintResources resources = resourceController.getResource(RESOURCENAME, 0, 0, 0);
        assertEquals(resources.getTotalItems(), 100);
        assertEquals(resources.getContent().size(), 100);
        assertEquals(resources.getSize(), 100);
    }

    @Test
    void testGetResourceSuccess_WhenSizeIsSet() {
        FintResources resources = resourceController.getResource(RESOURCENAME, 5, 0, 0);
        assertEquals(resources.getTotalItems(), 100);
        assertEquals(resources.getContent().size(), 5);
        assertEquals(resources.getSize(), 5);
    }

    @Test
    void testGetResourceSuccess_WhenOffsetIsSet() {
        FintResources resources = resourceController.getResource(RESOURCENAME, 5, 10, 0);
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
    void testStatusThrowsEventNotFoundException_WhenEventIsNotPresent() {
        assertThrows(EventNotFoundException.class, () -> resourceController.getStatus(WRITEABLE_RESOURCENAME, UUID.randomUUID().toString()));
    }

    @Test
    void testStatusThrowsEventFailedException_WhenResponseEventHasFailed() {
        eventService.registerResponse("123", ResponseFintEvent.builder().failed(true).build());
        assertThrows(EventFailedException.class, () -> resourceController.getStatus(WRITEABLE_RESOURCENAME, "123"));
    }

    @Test
    void testStatusThrowsEventRejectedException_WhenResponseEventIsRejected() {
        eventService.registerResponse("123", ResponseFintEvent.builder().rejected(true).build());
        assertThrows(EventRejectedException.class, () -> resourceController.getStatus(WRITEABLE_RESOURCENAME, "123"));
    }

    @Test
    void testStatusReturnsAccepted_WhenRequestIsPresent() {
        String corrId = "321123321";
        eventService.registerRequest(corrId);
        ResponseEntity<Object> status = resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId);
        assertEquals(HttpStatus.ACCEPTED, status.getStatusCode());
    }

    @Test
    void testGetStatusFailure_WhenResponseHasFailed() {
        String corrId = "123";
        FintResource eksamensgruppeResource = createElevResource(123123);

        eventService.registerRequest(corrId);
        eventService.registerResponse(corrId, createResponseFintEvent(eksamensgruppeResource, true, false));

        assertThrows(EventFailedException.class, () -> resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId));
    }

    @Test
    void testGetStatusFailure_WhenResponseIsRejected() {
        String corrId = "123";
        FintResource eksamensgruppeResource = createElevResource(123123);

        eventService.registerRequest(corrId);
        eventService.registerResponse(corrId, createResponseFintEvent(eksamensgruppeResource, false, true));

        assertThrows(EventRejectedException.class, () -> resourceController.getStatus(WRITEABLE_RESOURCENAME, corrId));
    }

    @Test
    void testPutResourceSuccess() {
        String corrId = "123";
        when(eventProducer.sendEvent(any(String.class), any(Object.class), any(OperationType.class)))
                .thenReturn(createRequestFintEvent(WRITEABLE_RESOURCENAME, corrId));

        ResponseEntity<Void> voidResponseEntity = resourceController.putResource(WRITEABLE_RESOURCENAME, "systemid", "", createElevResource(402));
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
                        WRITEABLE_RESOURCENAME, "NotAnIdField", "123", createElevResource(402))
        );
    }

    private RequestFintEvent createRequestFintEvent(String resourceName, String corrId) {
        return RequestFintEvent.builder()
                .resourceName(resourceName)
                .corrId(corrId)
                .build();
    }

    private ResponseFintEvent createResponseFintEvent(FintResource fintResource, boolean failed, boolean isRejected) {
        ResponseFintEvent responseFintEvent = new ResponseFintEvent();
        SyncPageEntry syncPageEntry = new SyncPageEntry();
        syncPageEntry.setIdentifier("321");
        syncPageEntry.setResource(fintResource);
        responseFintEvent.setFailed(failed);
        responseFintEvent.setRejected(isRejected);
        responseFintEvent.setValue(syncPageEntry);
        return responseFintEvent;
    }

    private FintResource createElevResource(int i) {
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
