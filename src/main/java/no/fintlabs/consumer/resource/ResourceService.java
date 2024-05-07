package no.fintlabs.consumer.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintIdentifikator;
import no.fint.model.FintResource;
import no.fint.model.resource.FintLinks;
import no.fintlabs.cache.Cache;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class ResourceService<T extends FintResource & FintLinks> {

    private final CacheService<T> cacheService;
    private final ReflectionService reflectionService;
    private final ObjectMapper objectMapper;

    public FintResource mapResource(String resourceName, String resourceString) {
        try {
            return objectMapper.readValue(resourceString, reflectionService.getResources().get(resourceName).clazz());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public int[] hashCodes(FintResource resource) {
        IntStream.Builder builder = IntStream.builder();

        resource.getIdentifikators().forEach((idField, identificator) -> {
            if (identificator != null && !identificator.getIdentifikatorverdi().isEmpty()) {
                builder.add(identificator.getIdentifikatorverdi().hashCode());
            }
        });

        return builder.build().toArray();
    }

    public Collection<T> getResources(String resource, int size, int offset, long sinceTimeStamp) {
        Stream<T> resources;
        Cache<T> cache = cacheService.getCache(resource);

        if (size > 0 && offset >= 0 && sinceTimeStamp > 0) {
            resources = cache.streamSliceSince(sinceTimeStamp, offset, size);
        } else if (size > 0 && offset >= 0) {
            resources = cache.streamSlice(offset, size);
        } else if (sinceTimeStamp > 0) {
            resources = cache.streamSince(sinceTimeStamp);
        } else {
            resources = cache.stream();
        }

        return resources.collect(Collectors.toList());
    }

    public Optional<T> getResourceById(String resourceName, String idField, String resourceIdValue) {
        return cacheService.getCache(resourceName).getLastUpdatedByFilter(resourceIdValue.hashCode(),
                resource -> Optional.ofNullable(resource)
                        .map(r -> r.getIdentifikators().get(idField.toLowerCase()))
                        .map(FintIdentifikator::getIdentifikatorverdi)
                        .map(resourceIdValue::equals)
                        .orElse(false)
        );
    }
}
