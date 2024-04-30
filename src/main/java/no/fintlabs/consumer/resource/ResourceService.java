package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintIdentifikator;
import no.fint.model.FintResource;
import no.fint.model.resource.FintLinks;
import no.fintlabs.cache.Cache;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class ResourceService<T extends FintResource & FintLinks> {

    private final CacheService<T> cacheService;

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
        String lowerCaseIdField = idField.toLowerCase();

        // TODO: Kom med en løsning på case problemet.

        return cacheService.getCache(resourceName).getLastUpdatedByFilter(resourceIdValue.hashCode(),
                (resource) -> {
                    if (resource != null) {
                        Map<String, FintIdentifikator> identifikators = resource.getIdentifikators();
                        Map<String, FintIdentifikator> lowerCaseIdentifikators = identifikators.entrySet().stream()
                                .collect(Collectors.toMap(
                                        entry -> entry.getKey().toLowerCase(),
                                        Map.Entry::getValue
                                ));
                        if (lowerCaseIdentifikators.containsKey(lowerCaseIdField)) {
                            return resourceIdValue.equals(lowerCaseIdentifikators.get(lowerCaseIdField).getIdentifikatorverdi());
                        }
                    }
                    return false;
                }
        );
    }
}
