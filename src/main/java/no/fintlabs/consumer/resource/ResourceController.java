package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintResource;
import no.fint.model.felles.kompleksedatatyper.Identifikator;
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
public class ResourceController {

    private final ResourceService resourceService;
    private final CacheService cacheService;

    // TODO: Make use of HATEOS -> fint-core-relations
    @GetMapping
    public Collection<FintResource> getResource(@PathVariable String resource,
                                                @RequestParam(defaultValue = "0") int size,
                                                @RequestParam(defaultValue = "0") int offset,
                                                @RequestParam(defaultValue = "0") long sinceTimeStamp) {
        return resourceService.getResources(resource, size, offset, sinceTimeStamp);
    }

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

    // TODO: Methods under requires Kafka

    @PostMapping
    public void postResource(@PathVariable String resource) {
        FravarResource fravarResource = new FravarResource();

        Identifikator identifikator1 = new Identifikator();
        identifikator1.setIdentifikatorverdi("321");
        fravarResource.setKildesystemId(identifikator1);

        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi("123");
        fravarResource.setSystemId(identifikator);


        log.info("Posting resource with ID: {}", identifikator.getIdentifikatorverdi());
        cacheService.getCache(resource).put("test", fravarResource, resourceService.hashCodes(fravarResource));
    }

    @PutMapping(BY_ID)
    public void putResourceById(@PathVariable String resource,
                                @PathVariable String idField,
                                @PathVariable String idValue) {
    }

}
