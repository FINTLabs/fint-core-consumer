package no.fintlabs.consumer.resource.integration;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.EksamensgruppeResource;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.adapter.models.ResponseFintEvent;
import no.fintlabs.adapter.models.SyncPageEntry;
import no.fintlabs.consumer.exception.EventFailedException;
import no.fintlabs.consumer.exception.EventRejectedException;
import no.fintlabs.consumer.exception.ResourceNotWriteableException;
import no.fintlabs.consumer.kafka.event.EventService;
import no.fintlabs.consumer.resource.ResourceController;
import no.fintlabs.consumer.resource.ResourceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ResourceControllerTest {

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private ResourceController resourceController;

    @Autowired
    private EventService eventService;

    // Mocking the kafka behaviour

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockBean
    private KafkaAdmin kafkaAdmin;

    private static final String RESOURCENAME = "elevfravar";
    private static final String WRITEABLE_RESOURCENAME = "eksamensgruppe";

    @BeforeEach
    public void setUp() {
        for (int i = 0; i < 100; i++) {
            resourceService.addResourceToCache(RESOURCENAME, String.valueOf(i), createElevFravarResource(i));
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
        assertThrows(ResourceNotWriteableException.class, () -> resourceController.postResource(RESOURCENAME, createElevFravarResource(101)));
    }

    @Test
    void testPostResourceSuccess() {
        ResponseEntity<?> responseEntity = resourceController.postResource("eksamensgruppe", new EksamensgruppeResource());
        String location = responseEntity.getHeaders().get("Location").getFirst();
        assertEquals(responseEntity.getStatusCode().value(), 201);
        assertTrue(location.startsWith("https://test.felleskomponent.no/utdanning/vurdering/eksamensgruppe/status/"));
    }

    @Test
    void testGetStatusSuccess() {
        String postResourceName = "eksamensgruppe";
        FintResource eksamensgruppeResource = createEksamensgruppeResource(321);

        ResponseEntity<?> responseEntity = resourceController.postResource(postResourceName, eksamensgruppeResource);
        String[] locationUriSplit = responseEntity.getHeaders().get("Location").getFirst().split("/");
        String corrId = locationUriSplit[locationUriSplit.length - 1];

        ResponseEntity<?> statusResponse = resourceController.getStatus(postResourceName, corrId);
        assertEquals(statusResponse.getStatusCode().value(), 202);

        eventService.registerRequest(corrId, new RequestFintEvent());
        statusResponse = resourceController.getStatus(postResourceName, corrId);
        assertEquals(statusResponse.getStatusCode().value(), 202);

        eventService.registerResponse(corrId, createResponseFintEvent(eksamensgruppeResource, false, false));
        statusResponse = resourceController.getStatus(postResourceName, corrId);
        String location = statusResponse.getHeaders().get("Location").getFirst();

        assertEquals(statusResponse.getStatusCode().value(), 201);
        assertEquals(location, "https://test.felleskomponent.no/utdanning/vurdering/eksamensgruppe/systemid/321");
    }

    @Test
    void testGetStatusFailure_WhenResponseHasFailed() {
        String postResourceName = "eksamensgruppe";
        FintResource eksamensgruppeResource = createEksamensgruppeResource(123123);

        ResponseEntity<?> responseEntity = resourceController.postResource(postResourceName, eksamensgruppeResource);
        String[] locationUriSplit = responseEntity.getHeaders().get("Location").getFirst().split("/");
        String corrId = locationUriSplit[locationUriSplit.length - 1];

        eventService.registerRequest(corrId, new RequestFintEvent());
        eventService.registerResponse(corrId, createResponseFintEvent(eksamensgruppeResource, true, false));

        assertThrows(EventFailedException.class, () -> resourceController.getStatus(postResourceName, corrId));
    }

    @Test
    void testGetStatusFailure_WhenResponseIsRejected() {
        String postResourceName = "eksamensgruppe";
        FintResource eksamensgruppeResource = createEksamensgruppeResource(123123);

        ResponseEntity<?> responseEntity = resourceController.postResource(postResourceName, eksamensgruppeResource);
        String[] locationUriSplit = responseEntity.getHeaders().get("Location").getFirst().split("/");
        String corrId = locationUriSplit[locationUriSplit.length - 1];

        eventService.registerRequest(corrId, new RequestFintEvent());
        eventService.registerResponse(corrId, createResponseFintEvent(eksamensgruppeResource, false, true));

        assertThrows(EventRejectedException.class, () -> resourceController.getStatus(postResourceName, corrId));
    }

    @Test
    void testPutResourceSuccess() {
        ResponseEntity<Void> voidResponseEntity = resourceController.putResource(WRITEABLE_RESOURCENAME, createEksamensgruppeResource(402));
        String location = voidResponseEntity.getHeaders().get("Location").getFirst();
        assertEquals(voidResponseEntity.getStatusCode().value(), 201);
        assertTrue(location.startsWith("https://test.felleskomponent.no/utdanning/vurdering/eksamensgruppe/status/"));
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

    private FintResource createEksamensgruppeResource(int i) {
        EksamensgruppeResource eksamensgruppeResource = new EksamensgruppeResource();
        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(String.valueOf(i));
        eksamensgruppeResource.setSystemId(identifikator);
        return eksamensgruppeResource;
    }

    private FintResource createElevFravarResource(int index) {
        ElevfravarResource elevfravarResource = new ElevfravarResource();
        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(String.valueOf(index));
        elevfravarResource.setSystemId(identifikator);
        elevfravarResource.addElevforhold(Link.with("systemid/test"));
        return elevfravarResource;
    }

}
