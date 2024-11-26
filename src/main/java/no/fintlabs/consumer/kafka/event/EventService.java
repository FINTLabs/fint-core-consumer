package no.fintlabs.consumer.kafka.event;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import no.fintlabs.adapter.models.sync.SyncPageEntry;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.event.EventFailedException;
import no.fintlabs.consumer.exception.event.EventNotFoundException;
import no.fintlabs.consumer.exception.event.EventRejectedException;
import no.fintlabs.consumer.resource.ResourceMapper;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventService {

    private final ConsumerConfiguration configuration;
    private final Cache<String, String> requestFintCorrIds;
    private final Cache<String, ResponseFintEvent> responseFintEvents;
    private final ResourceMapper resourceMapper;

    public boolean responseRecieved(String id) {
        ResponseFintEvent responseFintEvent = responseFintEvents.getIfPresent(id);
        boolean requestIsNotPresent = requestFintCorrIds.getIfPresent(id) == null;

        if (responseFintEvent == null) {
            if (requestIsNotPresent) {
                throw new EventNotFoundException(id, "no request or response was found");
            } else {
                log.debug("Event Status: 202 ACCEPTED, Corr-id: {}, Reason: {}", id, "RequestFintEvent is present but no ResponseFintEvent");
            }
            return false;
        }

        return switch (responseFintEvent) {
            case ResponseFintEvent failedEvent when failedEvent.isFailed() ->
                    throw new EventFailedException(responseFintEvent.getCorrId(), responseFintEvent.getErrorMessage());
            case ResponseFintEvent rejectedEvent when rejectedEvent.isRejected() ->
                    throw new EventRejectedException(responseFintEvent.getCorrId(), responseFintEvent.getRejectReason());
            default -> {
                log.debug("Event Status: 201 CREATED, Corr-id: {}, Reason: {}", id, "ResponseFintEvent is present");
                yield true;
            }
        };
    }

    public String getStatusHref(RequestFintEvent requestFintEvent) {
        return "%s/%s/status/%s".formatted(
                configuration.getComponentUrl(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getCorrId()
        );
    }

    public String createSelfHref(String resourceName, String corrId) {
        SyncPageEntry value = responseFintEvents.getIfPresent(corrId).getValue();
        if (value == null) {
            log.error("Error creating SelfLink because ResponseFintEvent SyncPageEntry is null");
            return null;
        } else if (value.getResource() != null) {
            log.error("Error creating SelfLink because ResponseFintEvent SyncPageEntry.getResource() is null");
            return null;
        }
        FintResource fintResource = getResourceFromEvent(resourceName, value.getResource());

        for (Map.Entry<String, FintIdentifikator> entry : fintResource.getIdentifikators().entrySet()) {
            if (entry.getValue().getIdentifikatorverdi() != null) {
                return "%s/%s/%s/%s".formatted(
                        configuration.getComponentUrl(),
                        resourceName,
                        entry.getKey().toLowerCase(),
                        entry.getValue().getIdentifikatorverdi()
                );
            }
        }

        log.error("Error creating selfLink because no identifikatorVerdi was set in resource");
        return null;
    }

    private FintResource getResourceFromEvent(String resourceName, Object resource) {
        return resourceMapper.mapResource(resourceName, resource);
    }

    public void registerRequest(String key) {
        requestFintCorrIds.put(key, key);
    }

    public void registerResponse(String key, ResponseFintEvent responseFintEvent) {
        responseFintEvents.put(key, responseFintEvent);
    }

    public Object getResource(String corrId) {
        return responseFintEvents.getIfPresent(corrId).getValue().getResource();
    }
}
