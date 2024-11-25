package no.fintlabs.consumer.kafka.event;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.event.EventFailedException;
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
                log.warn("Event: {} has no request!", id);
            } else {
                log.info("Event: {} has no response.", id);
            }
            return false;
        }

        return switch (responseFintEvent) {
            case ResponseFintEvent failedEvent when failedEvent.isFailed() ->
                    throw new EventFailedException(responseFintEvent.getCorrId(), responseFintEvent.getErrorMessage());
            case ResponseFintEvent rejectedEvent when rejectedEvent.isRejected() ->
                    throw new EventRejectedException(responseFintEvent.getCorrId(), responseFintEvent.getRejectReason());
            default -> true;
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
        FintResource fintResource = getResourceFromEvent(resourceName, responseFintEvents.getIfPresent(corrId).getValue().getResource());

        for (Map.Entry<String, FintIdentifikator> entry : fintResource.getIdentifikators().entrySet()) {
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

    private FintResource getResourceFromEvent(String resourceName, Object resource) {
        return resourceMapper.mapResource(resourceName, resource);
    }

    public void registerRequest(String key) {
        requestFintCorrIds.put(key, key);
    }

    public void registerResponse(String key, ResponseFintEvent responseFintEvent) {
        responseFintEvents.put(key, responseFintEvent);
    }

}
