package no.fintlabs.consumer.admin;

import lombok.RequiredArgsConstructor;
import no.fintlabs.consumer.CacheService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

import static no.fintlabs.consumer.config.Endpoints.ADMIN;

@RestController
@RequestMapping(ADMIN)
@RequiredArgsConstructor
public class AdminController {

    private final CacheService cacheService;

    @GetMapping("/health")
    public ResponseEntity<?> getHealthChecks() {
        // TODO: Implement when status service is working!
        return null;
    }

    @GetMapping("/cache/status")
    public Map<String, CacheEntry> getCacheStatus() {
        return cacheService.getResourceCaches().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new CacheEntry(new Date(entry.getValue().getLastUpdated()), entry.getValue().size())
                ));
    }

}
