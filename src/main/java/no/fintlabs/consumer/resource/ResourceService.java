package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.antlr.FintFilterService;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fintlabs.cache.Cache;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.kafka.KafkaHeader;
import no.fintlabs.consumer.links.LinkService;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class ResourceService {

    private final CacheService cacheService;
    private final LinkService linkService;
    private final ResourceMapper resourceMapper;
    private final FintFilterService oDataFilterService;

    public FintResource mapResourceAndLinks(String resourceName, Object object) {
        FintResource fintResource = resourceMapper.mapResource(resourceName, object);
        linkService.mapLinks(resourceName, fintResource);
        return fintResource;
    }

    public void addResourceToCache(String resourceName, String key, FintResource resource) {
        addResourceToCache(resourceName, key, resource, null);
    }

    public void addResourceToCache(String resourceName, String key, FintResource resource, Header header) {
        if (resource == null) {
            cacheService.getCache(resourceName).remove(key);
        } else {
            linkService.mapLinks(resourceName, resource);
            Cache<FintResource> cache = cacheService.getResourceCaches().get(resourceName);
            if (header == null) {
                log.debug("setting default entity retention: {} - {} - {}", resourceName, key, System.currentTimeMillis());
                cache.put(key, resource, hashCodes(resource));
            } else {
                long entityRetentionTime = KafkaHeader.getLong(header);
                log.debug("setting entity retention: {} - {} - {}", resourceName, key, entityRetentionTime);
                cache.put(key, resource, hashCodes(resource), entityRetentionTime);
            }
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

    public FintResources getResources(String resourceName, int size, int offset, long sinceTimeStamp, String filter) {
        Cache<FintResource> cache = cacheService.getCache(resourceName);
        Stream<FintResource> resourceStream = selectStream(cache, size, offset, sinceTimeStamp);
        resourceStream = applyFilter(resourceStream, filter);
        return linkService.toResources(resourceName, resourceStream, offset, size, cacheService.getSizeByResource(resourceName));
    }

    private Stream<FintResource> selectStream(Cache<FintResource> cache, int size, int offset, long sinceTimeStamp) {
        Stream<FintResource> resourceStream;

        if (size > 0 && offset >= 0 && sinceTimeStamp > 0) {
            resourceStream = cache.streamSliceSince(sinceTimeStamp, offset, size);
        } else if (size > 0 && offset >= 0) resourceStream = cache.streamSlice(offset, size);
        else if (sinceTimeStamp > 0) resourceStream = cache.streamSince(sinceTimeStamp);
        else resourceStream = cache.stream();

        return Objects.requireNonNull(resourceStream, "Cache implementation returned null stream");
    }

    private Stream<FintResource> applyFilter(Stream<FintResource> stream, String filter) {
        if (StringUtils.isBlank(filter)) return stream;

        if (!oDataFilterService.validate(filter)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid OData filter");
        }

        Stream<FintResource> filtered = oDataFilterService.from(stream, filter);
        return Objects.requireNonNull(filtered, "Filter service returned null stream");
    }

    // TODO: GetIdentifikators keyset is not lowercase, change this in fint-model
    public Optional<FintResource> getResourceById(String resourceName, String idField, String resourceIdValue) {
        return cacheService.getCache(resourceName.toLowerCase()).getLastUpdatedByFilter(resourceIdValue.hashCode(),
                resource -> Optional.ofNullable(resource)
                        .map(r -> getIdentifikator(r, idField))
                        .map(FintIdentifikator::getIdentifikatorverdi)
                        .map(resourceIdValue::equals)
                        .orElse(false)
        );
    }

    // TODO: Make idFields return lowercased by default
    private FintIdentifikator getIdentifikator(FintResource r, String idField) {
        return r.getIdentifikators().entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(
                        entry -> entry.getKey().toLowerCase(),
                        Map.Entry::getValue
                ))
                .get(idField.toLowerCase());
    }

}
