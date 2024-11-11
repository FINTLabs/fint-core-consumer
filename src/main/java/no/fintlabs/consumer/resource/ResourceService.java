package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fintlabs.cache.Cache;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.links.LinkService;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Slf4j
@Service
public class ResourceService {

    private final CacheService cacheService;
    private final LinkService linkService;
    private final ResourceIdentifierCache resourceIdentifierCache;

    public void addResourceToCache(String resourceName, String key, FintResource resource) {
        if (resource == null) {
            cacheService.getCache(resourceName).remove(key);
        } else {
            linkService.mapLinks(resourceName, resource);
            cacheService.getResourceCaches()
                    .get(resourceName)
                    .put(key, resource, calculateHashCodes(resource));
        }
    }

    private int[] calculateHashCodes(FintResource resource) {
        return resource.getIdentifikators().values().stream()
                .filter(this::identificatorIsPresent)
                .mapToInt(identificator -> identificator.getIdentifikatorverdi().hashCode())
                .toArray();
    }

    private boolean identificatorIsPresent(FintIdentifikator identificator) {
        return identificator != null && identificator.getIdentifikatorverdi() != null && !identificator.getIdentifikatorverdi().isEmpty();
    }

    public FintResources getResources(String resource, int size, int offset, long sinceTimeStamp) {
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

        return linkService.toResources(resource, resources, offset, size, cacheService.getSizeByResource(resource));
    }

    public Optional<FintResource> getResourceById(String resourceName, String idField, String resourceIdValue) {
        return cacheService.getCache(resourceName.toLowerCase()).getLastUpdatedByFilter(resourceIdValue.hashCode(),
                resource -> Optional.ofNullable(resource)
                        .map(r -> r.getIdentifikators().get(
                                resourceIdentifierCache.getIdField(resourceName, idField))
                        )
                        .map(FintIdentifikator::getIdentifikatorverdi)
                        .map(resourceIdValue::equals)
                        .orElse(false)
        );
    }

}
