package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintResource;
import no.fintlabs.cache.CacheContainer;
import org.springframework.web.bind.annotation.*;

import static no.fintlabs.consumer.config.Endpoints.*;

@RestController
@RequestMapping(RESOURCE_ENDPOINT)
@RequiredArgsConstructor
@Slf4j
public class ResourceController {

    private final CacheContainer cacheContainer;

    @GetMapping
    public void getResource(@PathVariable String resource) {

    }

    @GetMapping(BY_ID)
    public FintResource getResourceById(@PathVariable String resource,
                                        @PathVariable String idField,
                                        @PathVariable String idValue) {
        return null;
    }

    @GetMapping(LAST_UPDATED)
    public void getLastUpdated(@PathVariable String resource) {

    }

    @GetMapping(CACHE_SIZE)
    public void getResourceCacheSize(@PathVariable String resource) {

    }

    @PostMapping
    public void postResource(@PathVariable String resource) {

    }

    @PutMapping(BY_ID)
    public void putResourceById(@PathVariable String resource,
                                @PathVariable String idField,
                                @PathVariable String idValue) {

    }

}
