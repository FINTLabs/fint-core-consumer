package no.fintlabs.consumer.resource;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.cache.CacheService;
import no.fintlabs.cache.FintCache;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.kafka.entity.EntityConsumerRecord;
import no.fintlabs.consumer.kafka.event.RelationRequestProducer;
import no.fintlabs.consumer.kafka.sync.SyncTrackerService;
import no.fintlabs.consumer.links.LinkService;
import no.fintlabs.consumer.links.relation.RelationService;
import no.fintlabs.model.resource.FintResources;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.stream.Stream;

import static no.fintlabs.autorelation.model.RelationRequestKt.createDeleteRequest;

@Slf4j
@Service
@RequiredArgsConstructor
public class ResourceService {

    private final LinkService linkService;
    private final CacheService cacheService;
    private final RelationService relationService;
    private final ResourceMapperService resourceMapper;
    private final RelationRequestProducer relationRequestProducer;
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
        FintResource fintResource = resourceMapper.mapResource(resourceName, object);
        linkService.mapLinks(resourceName, fintResource);
        return fintResource;
    }

    private void deleteEntity(EntityConsumerRecord entityConsumerRecord) {
        FintCache<FintResource> cache = cacheService.getCache(entityConsumerRecord.getResourceName());
        FintResource fintResource = cache.get(entityConsumerRecord.getKey());

        if (fintResource != null) {
            publishDeleteRequestToKafka(entityConsumerRecord.getResourceName(), fintResource);
        }

        cache.remove(entityConsumerRecord.getKey(), entityConsumerRecord.getTimestamp());
    }

    private void publishDeleteRequestToKafka(String resourceName, FintResource resource) {
        relationRequestProducer.publish(
                createDeleteRequest(
                        consumerConfiguration.getOrgId(),
                        consumerConfiguration.getDomain(),
                        consumerConfiguration.getPackageName(),
                        resourceName,
                        resource
                )
        );
    }

    private void addToCache(EntityConsumerRecord entityConsumerRecord) {
        Objects.requireNonNull(entityConsumerRecord.getResource());
        FintCache<FintResource> cache = cacheService.getCache(entityConsumerRecord.getResourceName());

        relationService.handleLinks(entityConsumerRecord.getResourceName(), entityConsumerRecord.getKey(), entityConsumerRecord.getResource());
        linkService.mapLinks(entityConsumerRecord.getResourceName(), entityConsumerRecord.getResource());

        cache.put(entityConsumerRecord.getKey(), entityConsumerRecord.getResource(), entityConsumerRecord.getTimestamp());
    }

    public FintResources getResources(String resourceName, int size, int offset, long sinceTimeStamp, String filter) {
        FintCache<FintResource> cache = cacheService.getCache(resourceName);
        Stream<FintResource> resourceStream = cache.getStream(size, offset, sinceTimeStamp, filter);
        return linkService.toResources(resourceName, resourceStream, offset, size, cacheService.getCache(resourceName).getSize());
    }

    public FintResource getResourceById(String resourceName, String idField, String idValue) {
        return cacheService.getCache(resourceName).getByIdField(idField, idValue);
    }

}
