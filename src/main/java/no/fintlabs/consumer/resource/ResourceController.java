package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.operation.OperationType;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.kafka.event.EventProducer;
import no.fintlabs.consumer.resource.aspect.IdFieldCheck;
import no.fintlabs.consumer.resource.aspect.WriteableResource;
import no.fintlabs.consumer.resource.event.EventStatusService;
import no.fintlabs.model.resource.FintResources;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

import static no.fintlabs.consumer.config.EndpointsConstants.*;

@RestController
@RequestMapping("/{resource}")
@RequiredArgsConstructor
@Slf4j
public class ResourceController {

    private final ResourceService resourceService;
    private final CacheService cacheService;
    private final EventProducer eventProducer;
    private final EventStatusService eventStatusService;

    @GetMapping
    public FintResources getResource(
            @PathVariable String resource,
            @RequestParam(defaultValue = "0") int size,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "0") long sinceTimeStamp,
            @RequestParam(required = false) String $filter
    ) {
        return resourceService.getResources(resource, size, offset, sinceTimeStamp, $filter);
    }

    @PostMapping("/$query")
    public FintResources getResourceByOdataFilter(
            @PathVariable String resource,
            @RequestParam(defaultValue = "0") int size,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "0") long sinceTimeStamp,
            @RequestBody(required = false) String $filter
    ) {
        return getResource(resource, size, offset, sinceTimeStamp, $filter);
    }

    @IdFieldCheck
    @GetMapping(BY_ID)
    public FintResource getResourceById(
            @PathVariable String resource,
            @PathVariable String idField,
            @PathVariable String idValue
    ) {
        FintResource fintResource = resourceService.getResourceById(resource, idField, idValue);
        if (fintResource == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        return fintResource;
    }

    @GetMapping(LAST_UPDATED)
    public Map<String, Long> getLastUpdated(@PathVariable String resource) {
        return Map.of("lastUpdated", cacheService.getCache(resource).getLastUpdated());
    }

    @GetMapping(CACHE_SIZE)
    public Map<String, Integer> getResourceCacheSize(@PathVariable String resource) {
        return Map.of("size", cacheService.getCache(resource).getSize());
    }

    @WriteableResource
    @GetMapping(STATUS_ID)
    public ResponseEntity<Object> getStatus(@PathVariable String resource, @PathVariable String corrId) {
        return eventStatusService.getStatusResponse(resource, corrId);
    }

    @WriteableResource
    @PostMapping
    public ResponseEntity<Void> postResource(
            @PathVariable String resource,
            @RequestBody Object resourceData,
            @RequestParam(name = "validate", required = false) boolean validate
    ) {
        RequestFintEvent requestFintEvent = eventProducer.sendEvent(resource, resourceData, validate ? OperationType.VALIDATE : OperationType.CREATE);
        return ResponseEntity.accepted().header(HttpHeaders.LOCATION, eventStatusService.getStatusHref(requestFintEvent)).build();
    }

    @IdFieldCheck
    @WriteableResource
    @PutMapping(BY_ID)
    public ResponseEntity<Void> putResource(
            @PathVariable String resource,
            @PathVariable String idField,
            @PathVariable String idValue,
            @RequestBody Object resourceData
    ) {
        RequestFintEvent requestFintEvent = eventProducer.sendEvent(resource, resourceData, OperationType.UPDATE);
        return ResponseEntity.accepted().header(HttpHeaders.LOCATION, eventStatusService.getStatusHref(requestFintEvent)).build();
    }

}
