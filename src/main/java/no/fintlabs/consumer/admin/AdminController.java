package no.fintlabs.consumer.admin;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

import static no.fintlabs.consumer.config.Endpoints.ADMIN;

@RestController
@RequestMapping(ADMIN)
public class AdminController {

    @GetMapping("/health")
    public ResponseEntity<?> getHealthChecks() {
        // TODO: Implement when status service is working!
        return null;
    }

    @GetMapping("/cache/status")
    public Map<String, CacheEntry> getCacheStatus() {
        return null;
    }

}
