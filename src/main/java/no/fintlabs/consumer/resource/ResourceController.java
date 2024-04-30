package no.fintlabs.consumer.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintResource;
import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintLinks;
import no.fint.model.resource.administrasjon.personal.FravarResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;

import static no.fintlabs.consumer.config.Endpoints.*;

@RestController
@RequestMapping(RESOURCE_ENDPOINT)
@RequiredArgsConstructor
@Slf4j
public class ResourceController<T extends FintResource & FintLinks> {

    private final ResourceService<T> resourceService;
    private final CacheService<T> cacheService;

    // TODO: Make use of HATEOS -> fint-core-relations
    @GetMapping
    public Collection<T> getResource(@PathVariable String resource,
                                                @RequestParam(defaultValue = "0") int size,
                                                @RequestParam(defaultValue = "0") int offset,
                                                @RequestParam(defaultValue = "0") long sinceTimeStamp) {
        return resourceService.getResources(resource, size, offset, sinceTimeStamp);
    }

    @GetMapping(BY_ID)
    public ResponseEntity<T> getResourceById(@PathVariable String resource,
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

    // TODO: Methods under requires Kafka
    @PostMapping
    public void postResource(@PathVariable String resource, @RequestBody T thing) {
        // Objectmaper av klassen
        // Deseraliser data

        log.info(thing.toString());
    }

    @PutMapping(BY_ID)
    public void putResourceById(@PathVariable String resource,
                                @PathVariable String idField,
                                @PathVariable String idValue) {
    }

}
