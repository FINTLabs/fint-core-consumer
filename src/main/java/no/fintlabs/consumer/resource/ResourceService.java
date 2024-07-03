package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fintlabs.cache.Cache;
import no.fintlabs.consumer.CacheService;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Slf4j
@Service
public class ResourceService {

    private final CacheService cacheService;

    public void addResourceToCache(String resourceName, String key, FintResource resource) {
        if (resource == null) {
            cacheService.getCache(resourceName).remove(key);
        } else {
            cacheService.getResourceCaches().get(resourceName).put(key, resource, hashCodes(resource));
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

    public Collection<FintResource> getResources(String resource, int size, int offset, long sinceTimeStamp) {
        Stream<FintResource> resources;
        Cache<FintResource> cache = cacheService.getCache(resource);

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

    // TODO: GetIdentifikators keyset is not lowercase, change this in fint-model
    public Optional<FintResource> getResourceById(String resourceName, String idField, String resourceIdValue) {
        return cacheService.getCache(resourceName).getLastUpdatedByFilter(resourceIdValue.hashCode(),
                resource -> Optional.ofNullable(resource)
                        .map(r -> r.getIdentifikators().get(idField))
                        .map(FintIdentifikator::getIdentifikatorverdi)
                        .map(resourceIdValue::equals)
                        .orElse(false)
        );
    }

}
