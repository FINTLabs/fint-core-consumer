package no.fintlabs.consumer.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ResourceMapperService {

    private final ObjectMapper objectMapper;
    private final ResourceContext resourceContext;

    public FintResource mapResource(String resourceName, Object resource) {
        if (resource == null) {
            return null;
        }

        try {
            return objectMapper.convertValue(resource, resourceContext.getResource(resourceName).clazz());
        } catch (Exception e) {
            log.error("ObjectMapper failed to map resource: {} from: {}", resourceName, resource);
            throw new RuntimeException(e);
        }
    }

}
