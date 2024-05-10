package no.fintlabs.consumer.resource;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.adapter.models.ResponseFintEvent;
import no.fintlabs.consumer.exception.EventFailedException;
import no.fintlabs.consumer.exception.EventRejectedException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
@Slf4j
public class EventStatusService {

    // TODO: Evict old responses / implement better caching

    private final Set<String> recievedRequestIds = new HashSet<>();
    private final Map<String, ResponseFintEvent<String>> responseCache = new HashMap<>();

    public boolean responseRecieved(String id) {
        ResponseFintEvent responseFintEvent = responseCache.get(id);
        boolean requestIsNotPresent = !recievedRequestIds.contains(id);

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

    public void registerRequest(String key) {
        recievedRequestIds.add(key);
    }

    public void registerResponse(String key, ResponseFintEvent responseFintEvent) {
        responseCache.put(key, responseFintEvent);
    }

}
