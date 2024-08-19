package no.fintlabs.consumer.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ResourceMapper {

    private final ObjectMapper objectMapper;
    private final ResourceContext resourceContext;

    public FintResource mapResource(String resourceName, Object resource) {
        try {
            return objectMapper.convertValue(resource, resourceContext.getFintResourceInformationMap().get(resourceName).clazz());
        } catch (Exception e) {
            log.error("ObjectMapper failed to readValue from: {}", resource);
            throw new RuntimeException(e);
        }
    }

}
