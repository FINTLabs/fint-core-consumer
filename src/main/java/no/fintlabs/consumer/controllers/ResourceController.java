package no.fintlabs.consumer.controllers;

import org.springframework.web.bind.annotation.*;

import static no.fintlabs.consumer.config.Endpoints.*;
import static no.fintlabs.consumer.config.Endpoints.BY_ID;

@RestController
@RequestMapping(RESOURCE_ENDPOINT)
public class ResourceController {

    @GetMapping
    public void getResource(@PathVariable String resource) {

    }

    @GetMapping(BY_ID)
    public void getResourceById(@PathVariable String resource,
                                @PathVariable String idField,
                                @PathVariable String idValue) {

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
