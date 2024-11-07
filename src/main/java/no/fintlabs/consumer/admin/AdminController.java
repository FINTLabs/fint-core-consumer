package no.fintlabs.consumer.admin;

import lombok.RequiredArgsConstructor;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

import static no.fintlabs.consumer.config.EndpointsConstants.ADMIN;

@RestController
@RequestMapping(ADMIN)
@RequiredArgsConstructor
public class AdminController {

    private final CacheService cacheService;
    private final ConsumerConfiguration configuration;

    @GetMapping("/health")
    public ResponseEntity<?> getHealthChecks() {
        // TODO: Implement when status service is working!
        return null;
    }

    @Deprecated
    @GetMapping({"/organisations"})
    public Collection<String> getOrganisations() {
        return new ArrayList<>();
    }

    @Deprecated
    @GetMapping("/organisations/{orgId:.+}")
    public Collection<String> getOrganization(@PathVariable String orgId) {
        return new ArrayList<>();
    }

    @GetMapping("/assets")
    public Collection<String> getAssets() {
        return new HashSet<>(List.of(configuration.getOrgId()));
    }

    @Deprecated
    @GetMapping("/caches")
    public Map<String, Integer> getCaches() {
        return new HashMap<>();
    }

    @GetMapping("/cache/status")
    public Map<String, CacheEntry> getCacheStatus() {
        return cacheService.getResourceCaches().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new CacheEntry(new Date(entry.getValue().getLastUpdated()), entry.getValue().size())
                ));
    }

    @PostMapping({"/cache/rebuild", "/cache/rebuild/{model}"})
    public void rebuildCache(
            @RequestHeader(name = "x-client") String client,
            @PathVariable(required = false) String model
    ) {
        // TODO: Yet to be implemented
    }

}
