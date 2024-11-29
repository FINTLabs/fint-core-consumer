package no.fintlabs.consumer.resource.event;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import no.fintlabs.consumer.resource.ResourceMapper;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventService {

    private final Cache<String, String> requestFintCorrIds;
    private final Cache<String, ResponseFintEvent> responseFintEvents;
    private final ResourceMapper resourceMapper;

    public void registerResponse(String key, ResponseFintEvent responseFintEvent) {
        responseFintEvents.put(key, responseFintEvent);
    }

    public void registerRequest(String key) {
        requestFintCorrIds.put(key, key);
    }

    public ResponseFintEvent getResponse(String corrId) {
        return responseFintEvents.getIfPresent(corrId);
    }

    public boolean requestExists(String corrId) {
        return requestFintCorrIds.getIfPresent(corrId) != null;
    }

    public FintResource getResource(String resourceName, String corrId) {
        return resourceMapper.mapResource(resourceName, responseFintEvents.getIfPresent(corrId).getValue().getResource());
    }

}
