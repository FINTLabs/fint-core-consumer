package no.fintlabs.consumer.kafka.entity

import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.MetricService
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.sync.SyncTrackerService
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceLockService
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service

@Service
class EntityProcessingService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val autoRelationService: AutoRelationService,
    private val consumerConfiguration: ConsumerConfiguration,
    private val syncTrackerService: SyncTrackerService,
    private val resourceLockService: ResourceLockService,
    private val metricService: MetricService,
) {
    /**
     * Two-phase to avoid cross-resource lock deadlock:
     *  1. Mutate this resource's own document while holding only its lock.
     *  2. Apply relation changes to OTHER documents afterwards — each target takes only its own
     *     lock (in [AutoRelationService.process]), so no thread ever holds two document locks.
     */
    fun processEntityConsumerRecord(record: EntityConsumerRecord) {
        var removed: FintResource? = null
        resourceLockService.withLock(record.resourceKey, record.key) {
            if (record.resource == null) {
                removed = deleteEntity(record)
            } else {
                addToCache(record)
            }

            if (record.type != null) {
                syncTrackerService.processRecordMetadata(record)
            }
        }

        if (consumerConfiguration.autorelation.enabled) {
            applyRelationsOutsideLock(record, removed)
        }
    }

    private fun applyRelationsOutsideLock(
        record: EntityConsumerRecord,
        removed: FintResource?,
    ) {
        val resource = record.resource
        if (resource != null) {
            autoRelationService.applyRelations(record.resourceKey, record.key, resource)
        } else if (removed != null) {
            autoRelationService.applyRemoval(record.resourceKey, record.key, removed)
        }
    }

    private fun deleteEntity(record: EntityConsumerRecord): FintResource? {
        val cache = cacheService.getCache(record.resourceKey)
        val existing = cache.get(record.key)
        cache.remove(record.key, record.timestamp)
        return existing
    }

    private fun addToCache(record: EntityConsumerRecord) {
        val resource = requireNotNull(record.resource)
        val cache = cacheService.getCache(record.resourceKey)

        if (consumerConfiguration.autorelation.enabled) {
            autoRelationService.reconcileLinks(record.resourceKey, record.key, resource)
        }

        linkService.mapLinks(record.resourceKey, resource)
        val accepted = cache.put(record.key, resource, record.timestamp)
        if (!accepted) {
            metricService.incrementCachePutRejectedOlderTimestamp(record.resourceKey)
        }
    }
}
