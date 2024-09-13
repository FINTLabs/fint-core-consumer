package no.fintlabs.consumer.kafka.event;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.adapter.models.ResponseFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.EventFailedException;
import no.fintlabs.consumer.exception.EventRejectedException;
import no.fintlabs.consumer.resource.ResourceMapper;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventService {

    private final ConsumerConfiguration configuration;
    private final Cache<String, RequestFintEvent> requestFintEvents;
    private final Cache<String, ResponseFintEvent> responseFintEvents;
    private final ResourceMapper resourceMapper;

    public boolean responseRecieved(String id) {
        ResponseFintEvent responseFintEvent = responseFintEvents.getIfPresent(id);
        boolean requestIsNotPresent = requestFintEvents.getIfPresent(id) == null;

        if (responseFintEvent == null && requestIsNotPresent) {
            log.warn("Event: {} has no request!", id);
            return false;
        } else if (responseFintEvent == null) {
            log.info("Event: {} has no response.", id);
            return false;
        } else if (responseFintEvent.isFailed()) {
            log.info("EventResponse: {} has failed: {}", id, responseFintEvent.getErrorMessage());
            throw new EventFailedException(responseFintEvent.getErrorMessage());
        } else if (responseFintEvent.isRejected()) {
            log.info("EventResponse: {} is rejected: {}", id, responseFintEvent.getErrorMessage());
            throw new EventRejectedException(responseFintEvent.getErrorMessage(), responseFintEvent.getRejectReason());
        }

        return true;
    }

    public String getStatusHref(RequestFintEvent requestFintEvent) {
        return "%s/%s/status/%s".formatted(
                configuration.getComponentUrl(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getCorrId()
        );
    }

    public String createFirstSelfHref(String resourceName, FintResource resource) {
        for (Map.Entry<String, FintIdentifikator> entry : resource.getIdentifikators().entrySet()) {
            if (entry.getValue() != null && entry.getValue().getIdentifikatorverdi() != null) {
                return "%s/%s/%s/%s".formatted(
                        configuration.getComponentUrl(),
                        resourceName,
                        entry.getKey().toLowerCase(),
                        entry.getValue().getIdentifikatorverdi()
                );
            }
        }

        return null;
    }

    public void registerRequest(String key, RequestFintEvent requestFintEvent) {
        requestFintEvents.put(key, requestFintEvent);
    }

    public void registerResponse(String key, ResponseFintEvent responseFintEvent) {
        responseFintEvents.put(key, responseFintEvent);
    }

    public FintResource getResource(String resourceName, String id) {
        return resourceMapper.mapResource(resourceName, responseFintEvents.getIfPresent(id).getValue().getResource());
    }
}
