package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.OperationType;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.CacheService;
import no.fintlabs.consumer.LinkService;
import no.fintlabs.consumer.kafka.event.EventProducer;
import no.fintlabs.consumer.kafka.event.EventService;
import no.fintlabs.consumer.resource.aspect.IdFieldCheck;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;

import static no.fintlabs.consumer.config.Endpoints.*;

@RestController
@RequestMapping(RESOURCE_ENDPOINT)
@RequiredArgsConstructor
@Slf4j
public class ResourceController {

    private final ResourceService resourceService;
    private final CacheService cacheService;
    private final EventProducer eventProducer;
    private final EventService eventService;
    private final LinkService linkService;

    // TODO: Make use of HATEOS -> fint-core-relations
    @GetMapping
    public Collection<FintResource> getResource(@PathVariable String resource,
                                                @RequestParam(defaultValue = "0") int size,
                                                @RequestParam(defaultValue = "0") int offset,
                                                @RequestParam(defaultValue = "0") long sinceTimeStamp) {
        return resourceService.getResources(resource, size, offset, sinceTimeStamp);
    }

    @IdFieldCheck
    @GetMapping(BY_ID)
    public ResponseEntity<FintResource> getResourceById(@PathVariable String resource,
                                                        @PathVariable String idField,
                                                        @PathVariable String idValue) {
        return resourceService.getResourceById(resource, idField, idValue)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping(LAST_UPDATED)
    public Map<String, Long> getLastUpdated(@PathVariable String resource) {
        return Map.of("lastUpdated", cacheService.getCache(resource).getLastUpdated());
    }

    @GetMapping(CACHE_SIZE)
    public Map<String, Integer> getResourceCacheSize(@PathVariable String resource) {
        return Map.of("size", cacheService.getCache(resource).size());
    }

    // TODO: Implement an aspect to check if the resource is writeable & gain access to the methods under this

    @GetMapping(STATUS_ID)
    public ResponseEntity<?> getStatus(@PathVariable String resource, @PathVariable String id) {
        return eventService.responseRecieved(id)
                ? ResponseEntity.created(linkService.createSelfHref(resource, eventService.getResource(resource, id))).build()
                : ResponseEntity.accepted().build();
    }

    @PostMapping
    public ResponseEntity<?> postResource(@PathVariable String resource, @RequestBody Object resourceData) {
        RequestFintEvent requestFintEvent = eventProducer.sendEvent(resource, resourceData, OperationType.CREATE);
        return ResponseEntity.created(linkService.createSelfHref(requestFintEvent)).build();
    }

    @PutMapping
    public void putResource(@PathVariable String resource, @RequestBody String resourceData) {
        eventProducer.sendEvent(resource, resourceData, OperationType.UPDATE);
    }

}
