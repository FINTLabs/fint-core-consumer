package no.fintlabs.consumer.resource;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
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
import java.util.UUID;

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

    @PostConstruct
    public void init() {
        ElevfravarResource elevResource = new ElevfravarResource();
        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi("123");
        elevResource.setSystemId(identifikator);
        elevResource.addElevforhold(Link.with("/systemid/321"));


        resourceService.addResourceToCache("elevfravar", UUID.randomUUID().toString(), elevResource);
    }

    @GetMapping
    public FintResources getResource(@PathVariable String resource,
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

    @WriteableResource
    @GetMapping(STATUS_ID)
    public ResponseEntity<?> getStatus(@PathVariable String resource, @PathVariable String id) {
        return eventService.responseRecieved(id)
                ? ResponseEntity.created(URI.create(eventService.createFirstSelfHref(resource, eventService.getResource(resource, id)))).build()
                : ResponseEntity.accepted().build();
    }

    @WriteableResource
    @PostMapping
    public ResponseEntity<?> postResource(@PathVariable String resource, @RequestBody Object resourceData) {
        RequestFintEvent requestFintEvent = eventProducer.sendEvent(resource, resourceData, OperationType.CREATE);
        return ResponseEntity.created(URI.create(eventService.getStatusHref(requestFintEvent))).build();
    }

    @WriteableResource
    @PutMapping
    public void putResource(@PathVariable String resource, @RequestBody String resourceData) {
        eventProducer.sendEvent(resource, resourceData, OperationType.UPDATE);
    }

}
