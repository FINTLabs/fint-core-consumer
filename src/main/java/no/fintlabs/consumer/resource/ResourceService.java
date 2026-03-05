package no.fintlabs.consumer.resource;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.antlr.FintFilterService;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fintlabs.cache.Cache;
import no.fintlabs.cache.CacheResourceLockService;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.kafka.entity.ConsumerRecordMetadata;
import no.fintlabs.consumer.kafka.entity.KafkaEntity;
import no.fintlabs.consumer.kafka.event.RelationRequestProducer;
import no.fintlabs.consumer.kafka.sync.SyncTrackerService;
import no.fintlabs.consumer.links.LinkService;
import no.fintlabs.consumer.links.relation.RelationService;
import no.fintlabs.model.resource.FintResources;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
    private final FintFilterService oDataFilterService;
    private final RelationRequestProducer relationRequestProducer;
    private final ConsumerConfiguration consumerConfiguration;
    private final SyncTrackerService syncTrackerService;
    private final CacheResourceLockService cacheResourceLockService;
    private final MeterRegistry meterRegistry;

    public void processEntityConsumerRecord(KafkaEntity entityConsumerRecord) {
        String resourceName = entityConsumerRecord.getResourceName();
        timed(resourceName, "record.process.total", () -> {
            timed(resourceName, "cache.updateRetentionTime",
                    () -> cacheResourceLockService.withLock(
                            resourceName,
                            () -> cacheService.updateRetentionTime(resourceName, entityConsumerRecord.getRetentionTime())
                    ));

            if (entityConsumerRecord.getResource() == null) {
                timed(resourceName, "record.deletePath", () -> deleteEntity(entityConsumerRecord));
            } else {
                timed(resourceName, "record.addPath", () -> addToCache(entityConsumerRecord));
            }

            // Track sync status and evict cache if full sync is completed
            ConsumerRecordMetadata recordMetadata = entityConsumerRecord.getConsumerRecordMetadata();
            if (recordMetadata != null) {
                timed(resourceName, "sync.processRecordMetadata",
                        () -> syncTrackerService.processRecordMetadata(resourceName, recordMetadata));
            }
        });
    }

    public FintResource mapResourceAndLinks(String resourceName, Object object) {
        FintResource fintResource = timed(resourceName, "resource.map",
                () -> resourceMapper.mapResource(resourceName, object));
        timed(resourceName, "links.map", () -> linkService.mapLinks(resourceName, fintResource));
        return fintResource;
    }

    private void deleteEntity(KafkaEntity kafkaEntity) {
        String resourceName = kafkaEntity.getResourceName();
        Cache<FintResource> cache = timed(resourceName, "cache.getCache", () -> cacheService.getCache(resourceName));
        FintResource fintResource = timed(resourceName, "cache.mutate.delete", () ->
                cacheResourceLockService.withLock(resourceName, () -> {
                    FintResource existing = timed(resourceName, "cache.get", () -> cache.get(kafkaEntity.getKey()));
                    timed(resourceName, "cache.remove", () -> cache.remove(kafkaEntity.getKey()));
                    return existing;
                })
        );

        if (fintResource != null) {
            timed(resourceName, "kafka.publishDeleteRequest",
                    () -> publishDeleteRequestToKafka(resourceName, fintResource));
        }
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

    private void addToCache(KafkaEntity entity) {
        Objects.requireNonNull(entity.getResource());
        String resourceName = entity.getResourceName();
        Cache<FintResource> cache = timed(resourceName, "cache.getCache", () -> cacheService.getCache(resourceName));

        if (consumerConfiguration.getAutorelation()) {
            timed(resourceName, "autorelation.handleLinks",
                    () -> relationService.handleLinks(resourceName, entity.getKey(), entity.getResource()));
        }
        timed(resourceName, "links.map", () -> linkService.mapLinks(resourceName, entity.getResource()));

        int[] hashCodes = timed(resourceName, "resource.hashCodes", () -> hashCodes(entity.getResource()));
        timed(resourceName, "cache.put",
                () -> cacheResourceLockService.withLock(
                        resourceName,
                        () -> cache.put(entity.getKey(), entity.getResource(), hashCodes, entity.getLastModified())
                ));
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
        if (filter == null || filter.isBlank()) return stream;

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

    private void timed(String resourceName, String operation, Runnable runnable) {
        timed(resourceName, operation, () -> {
            runnable.run();
            return null;
        });
    }

    private <T> T timed(String resourceName, String operation, Supplier<T> supplier) {
        Timer.Sample sample = Timer.start(meterRegistry);
        String status = "success";
        try {
            return supplier.get();
        } catch (RuntimeException runtimeException) {
            status = "error";
            log.error(
                    "Processing component failed: operation={}, resource={}, org={}, status={}",
                    operation,
                    safeResourceName(resourceName),
                    consumerConfiguration.getOrgId(),
                    status,
                    runtimeException
            );
            throw runtimeException;
        } finally {
            Duration duration = Duration.ofNanos(sample.stop(timer(resourceName, operation, status)));
            if (duration.compareTo(SLOW_COMPONENT_THRESHOLD) > 0) {
                log.warn(
                        "Slow processing component detected: operation={}, durationMs={}, resource={}, org={}, status={}",
                        operation,
                        duration.toMillis(),
                        safeResourceName(resourceName),
                        consumerConfiguration.getOrgId(),
                        status
                );
            }
        }
    }

    private Timer timer(String resourceName, String operation, String status) {
        return Timer.builder("core.consumer.processing")
                .description("Duration of internal processing steps for Kafka entity records")
                .tag("org", consumerConfiguration.getOrgId())
                .tag("resource", safeResourceName(resourceName))
                .tag("operation", operation)
                .tag("status", status)
                .register(meterRegistry);
    }

    private String safeResourceName(String resourceName) {
        return resourceName == null || resourceName.isBlank() ? "unknown" : resourceName;
    }

    private static final Duration SLOW_COMPONENT_THRESHOLD = Duration.ofSeconds(10);

}
