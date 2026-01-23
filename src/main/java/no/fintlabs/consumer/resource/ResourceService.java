package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.antlr.FintFilterService;
import no.fint.model.resource.FintResource;
import no.fintlabs.autorelation.AutoRelationService;
import no.fintlabs.autorelation.RelationEventService;
import no.fintlabs.cache.CacheService;
import no.fintlabs.cache.FintCache;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.kafka.entity.ConsumerRecordMetadata;
import no.fintlabs.consumer.kafka.entity.EntityConsumerRecord;
import no.fintlabs.consumer.kafka.sync.SyncTrackerService;
import no.fintlabs.consumer.links.LinkService;
import no.fintlabs.model.resource.FintResources;
import no.novari.fint.model.FintIdentifikator;
import no.novari.fint.model.resource.FintResource;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class ResourceService {

    private final LinkService linkService;
    private final CacheService cacheService;
    private final AutoRelationService autoRelationService;
    private final RelationEventService relationEventService;
    private final ResourceConverter resourceConverter;
    private final FintFilterService oDataFilterService;
    private final ConsumerConfiguration consumerConfiguration;
    private final SyncTrackerService syncTrackerService;

    public void processEntityConsumerRecord(EntityConsumerRecord entityConsumerRecord) {
        if (entityConsumerRecord.getResource() == null) {
            deleteEntity(entityConsumerRecord);
        } else {
            addToCache(entityConsumerRecord);
        }

        // Track sync status and evict cache if full sync is completed
        if (entityConsumerRecord.getType() != null) {
            syncTrackerService.processRecordMetadata(entityConsumerRecord);
        }
    }

    public FintResource mapResourceAndLinks(String resourceName, Object object) {
        FintResource fintResource = resourceConverter.convert(resourceName, object);
        linkService.mapLinks(resourceName, fintResource);
        return fintResource;
    }

    private void deleteEntity(EntityConsumerRecord entityConsumerRecord) {
        FintCache<FintResource> cache = getCache(entityConsumerRecord.getResourceName());
        FintResource fintResource = cache.get(entityConsumerRecord.getKey());

        if (fintResource != null) {
            relationEventService.removeRelations(entityConsumerRecord.getResourceName(), entityConsumerRecord.getKey(), fintResource);
        }

        cache.remove(entityConsumerRecord.getKey(), entityConsumerRecord.getTimestamp());
    }

    private void addToCache(EntityConsumerRecord entityConsumerRecord) {
        Objects.requireNonNull(entityConsumerRecord.getResource());
        FintCache<FintResource> cache = getCache(entityConsumerRecord.getResourceName());

        if (consumerConfiguration.getAutorelation()) {
            autoRelationService.reconcileLinks(entity.getResourceName(), entity.getKey(), entity.getResource());
        }
        linkService.mapLinks(entityConsumerRecord.getResourceName(), entityConsumerRecord.getResource());

        cache.put(entityConsumerRecord.getKey(), entityConsumerRecord.getResource(), entityConsumerRecord.getTimestamp());
    }

    public FintResources getResources(String resourceName, int size, int offset, long sinceTimeStamp, String filter) {
        FintCache<FintResource> cache = getCache(resourceName);
        List<FintResource> resources = cache.getList(size, offset, sinceTimeStamp, filter);
        return linkService.toResources(resourceName, resources, offset, size, getCache(resourceName).getSize());
    }

    public FintResource getResourceById(String resourceName, String idField, String idValue) {
        return getCache(resourceName).getByIdField(idField, idValue);
    }

    public Long getLastUpdated(String resourceName) {
        return getCache(resourceName).getLastUpdated();
    }

    public int getCacheSize(String resourceName) {
        return getCache(resourceName).getSize();
    }

    @NotNull
    private FintCache<FintResource> getCache(String resourceName) {
        return cacheService.getCache(resourceName);
    }

}
