package no.fintlabs.consumer.kafka.event;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.adapter.models.ResponseFintEvent;
import no.fintlabs.consumer.exception.EventFailedException;
import no.fintlabs.consumer.exception.EventRejectedException;
import no.fintlabs.consumer.resource.ResourceMapper;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventService {

    // TODO: Evict old responses / implement better caching

    private final Cache<String, RequestFintEvent> requestFintEvents;
    private final Cache<String, ResponseFintEvent> responseFintEvents;
    private final ResourceMapper resourceMapper;

    public boolean responseRecieved(String id) {
        ResponseFintEvent responseFintEvent = responseFintEvents.getIfPresent(id);
        boolean requestIsNotPresent = requestFintEvents.getIfPresent(id) == null;

        if (responseFintEvent == null && requestIsNotPresent) {
            log.warn("EventResponse corrId: {} has no matching request!", id);
            return false;
        } else if (responseFintEvent == null) {
            log.info("EventResponse corrId: {} has no response yet.", id);
            return false;
        } else if (responseFintEvent.isFailed()) {
            log.info("EventResponse corrId: {} has failed: {}", id, responseFintEvent.getErrorMessage());
            throw new EventFailedException(responseFintEvent.getErrorMessage());
        } else if (responseFintEvent.isRejected()) {
            log.info("EventResponse corrId: {} is rejected: {}", id, responseFintEvent.getErrorMessage());
            throw new EventRejectedException(responseFintEvent.getErrorMessage(), responseFintEvent.getRejectReason());
        }

        return true;
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
