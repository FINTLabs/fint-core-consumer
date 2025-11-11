package no.fintlabs.consumer.resource.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.event.EventBodyResponse;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import no.fintlabs.adapter.operation.OperationType;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.links.LinkService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventStatusService {

    private final ConsumerConfiguration configuration;
    private final EventService eventService;
    private final LinkService linkService;

    public ResponseEntity<Object> getStatusResponse(String resourceName, String corrId) {
        ResponseFintEvent responseFintEvent = eventService.getResponse(corrId);

        if (responseFintEvent == null) {
            if (eventService.requestExists(corrId)) {
                logStatus("202 ACCEPTED", "No Response found");
                return ResponseEntity.accepted().build();
            }
            logStatus("401 NOT FOUND", "No Request or Response found");
            return ResponseEntity.status(HttpStatus.GONE).build();
        }

        if (responseFintEvent.getOperationType().equals(OperationType.VALIDATE)) {
            logStatus("200 OK", "Handled validate event");
            return ResponseEntity.ok(EventBodyResponse.ofResponseEvent(responseFintEvent));
        } else if (responseFintEvent.getOperationType().equals(OperationType.DELETE)) {
            logStatus("204 NO CONTENT", "Handled delete event");
            return ResponseEntity.noContent().build();
        } else if (responseFintEvent.isFailed()) {
            logStatus("500 INTERNAL SERVER ERROR", "Handled failed event");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(EventBodyResponse.ofResponseEvent(responseFintEvent));
        } else if (responseFintEvent.isRejected()) {
            logStatus("400 BAD REQUEST", "Handled rejected event");
            return ResponseEntity.badRequest().body(EventBodyResponse.ofResponseEvent(responseFintEvent));
        }

        FintResource fintResource = eventService.getResource(resourceName, corrId);
        linkService.mapLinks(resourceName, fintResource);

        if (responseFintEvent.isConflicted()) {
            logStatus("409 CONFLICT", "Handled conflicted event");
            return ResponseEntity.status(HttpStatus.CONFLICT).body(fintResource);
        }

        logStatus("201 CREATED", "Event successfully created");
        return ResponseEntity.created(createLocationUri(resourceName, fintResource)).body(fintResource);
    }

    public String getStatusHref(RequestFintEvent requestFintEvent) {
        return "%s/%s/status/%s".formatted(
                configuration.getComponentUrl(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getCorrId()
        );
    }

    private URI createLocationUri(String resourceName, FintResource fintResource) {
        for (Map.Entry<String, FintIdentifikator> entry : fintResource.getIdentifikators().entrySet()) {
            if (entry.getValue() != null && entry.getValue().getIdentifikatorverdi() != null) {
                return URI.create(
                        "%s/%s/%s/%s".formatted(
                                configuration.getComponentUrl(),
                                resourceName.toLowerCase(),
                                entry.getKey().toLowerCase(),
                                entry.getValue().getIdentifikatorverdi()
                        )
                );
            }
        }

        log.error("Error creating selfLink because no identifikatorVerdi was set in resource");
        return null;
    }

    private void logStatus(String status, String message) {
        log.info("Status-endpoint: {} - Cause: {}", status, message);
    }

}
