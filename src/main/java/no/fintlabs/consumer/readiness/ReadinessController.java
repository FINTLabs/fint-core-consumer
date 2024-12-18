package no.fintlabs.consumer.readiness;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientResponseException;

@Slf4j
@RestController
@RequiredArgsConstructor
public class ReadinessController {

    private final ReadinessService readinessService;

    @GetMapping("/ready")
    public ResponseEntity<Void> readinessStatus() {
        try {
            return readinessService.isReady()
                    ? ResponseEntity.ok().build()
                    : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        } catch (RestClientResponseException e) {
            log.error("Failed to connect to existing consumer: {}", e.getMessage());
            return ResponseEntity.ok().build();
        } catch (IllegalArgumentException e) {
            log.error("Offset validation failed: {}", e.getMessage());
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Unexpected error occured: {}", e.getMessage());
            return ResponseEntity.ok().build();
        }
    }

}
