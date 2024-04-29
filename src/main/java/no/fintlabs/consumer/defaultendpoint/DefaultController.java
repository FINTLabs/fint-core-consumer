package no.fintlabs.consumer.defaultendpoint;

import no.fintlabs.consumer.defaultendpoint.DefaultEndpoints;
import no.fintlabs.consumer.defaultendpoint.EndpointDetails;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

import static no.fintlabs.consumer.config.Endpoints.DEFAULT_ENDPOINT;

@RestController
@RequestMapping(DEFAULT_ENDPOINT)
@ComponentScan("no.fintlabs.reflection")
public class DefaultController {

    private final ReflectionService reflectionService;
    private final DefaultEndpoints defaultEndpoints;

    public DefaultController(ReflectionService reflectionService) {
        this.reflectionService = reflectionService;
        this.defaultEndpoints = createDefaultEndpoints();
    }

    @GetMapping
    public ResponseEntity<Map<String, EndpointDetails>> displayEndpoints() {
        return ResponseEntity.ok(defaultEndpoints.getDefaultEndpointDetails());
    }

    private DefaultEndpoints createDefaultEndpoints() {
        DefaultEndpoints defaultEndpoints = new DefaultEndpoints();

        assert reflectionService != null;
        reflectionService.getResources().forEach((resource, idFields) -> {
            defaultEndpoints.addEndpointDetails(resource, EndpointDetails.ofResource(resource, idFields));
        });

        return defaultEndpoints;
    }

}
