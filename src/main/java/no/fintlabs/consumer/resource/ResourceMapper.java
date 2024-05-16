package no.fintlabs.consumer.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ResourceMapper {

    private final ObjectMapper objectMapper;
    private final ReflectionService reflectionService;

    public FintResource mapResource(String resourceName, Object resource) {
        try {
            return objectMapper.convertValue(resource, reflectionService.getResources().get(resourceName).clazz());
        } catch (Exception e) {
            log.error("ObjectMapper failed to readValue from: {}", resource);
            throw new RuntimeException(e);
        }
    }

}
