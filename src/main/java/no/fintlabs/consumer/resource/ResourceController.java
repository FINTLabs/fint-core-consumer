package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fintlabs.adapter.models.OperationType;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.CacheService;
import no.fintlabs.consumer.kafka.event.EventProducer;
import no.fintlabs.consumer.kafka.event.EventService;
import no.fintlabs.consumer.resource.aspect.IdFieldCheck;
import no.fintlabs.consumer.resource.aspect.WriteableResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.Map;

import static no.fintlabs.consumer.config.Endpoints.*;

@RestController
@RequestMapping("/{resource}")
@RequiredArgsConstructor
@Slf4j
public class ResourceController {

    private final ResourceService resourceService;
    private final CacheService cacheService;
    private final EventProducer eventProducer;
    private final EventService eventService;

    @GetMapping
    public FintResources getResource(@PathVariable String resource,
                                     @RequestParam(defaultValue = "0") int size,
                                     @RequestParam(defaultValue = "0") int offset,
                                     @RequestParam(defaultValue = "0") long sinceTimeStamp) {
        return resourceService.getResources(resource.toLowerCase(), size, offset, sinceTimeStamp);
    }

    @IdFieldCheck
    @GetMapping(BY_ID)
    public ResponseEntity<FintResource> getResourceById(@PathVariable String resource,
                                                        @PathVariable String idField,
                                                        @PathVariable String idValue) {
        return resourceService.getResourceById(resource.toLowerCase(), idField, idValue)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping(LAST_UPDATED)
    public Map<String, Long> getLastUpdated(@PathVariable String resource) {
        return Map.of("lastUpdated", cacheService.getCache(resource.toLowerCase()).getLastUpdated());
    }

    @GetMapping(CACHE_SIZE)
    public Map<String, Integer> getResourceCacheSize(@PathVariable String resource) {
        return Map.of("size", cacheService.getCache(resource.toLowerCase()).size());
    }

    @WriteableResource
    @GetMapping(STATUS_ID)
    public ResponseEntity<Void> getStatus(@PathVariable String resource, @PathVariable String corrId) {
        return eventService.responseRecieved(corrId)
                ? ResponseEntity.created(URI.create(eventService.createSelfHref(resource.toLowerCase(), corrId))).build()
                : ResponseEntity.accepted().build();
    }

    @WriteableResource
    @PostMapping
    public ResponseEntity<Void> postResource(@PathVariable String resource, @RequestBody Object resourceData) {
        RequestFintEvent requestFintEvent = eventProducer.sendEvent(resource.toLowerCase(), resourceData, OperationType.CREATE);
        return ResponseEntity.created(URI.create(eventService.getStatusHref(requestFintEvent))).build();
    }

    @WriteableResource
    @PutMapping
    public ResponseEntity<Void> putResource(@PathVariable String resource, @RequestBody Object resourceData) {
        RequestFintEvent requestFintEvent = eventProducer.sendEvent(resource.toLowerCase(), resourceData, OperationType.UPDATE);
        return ResponseEntity.created(URI.create(eventService.getStatusHref(requestFintEvent))).build();
    }

}
