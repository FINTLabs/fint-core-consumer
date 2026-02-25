package no.fintlabs.consumer.kafka.entity

import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.RelationEventService
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.sync.SyncTrackerService
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceLockService
import org.springframework.stereotype.Service

@Service
class EntityProcessingService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val autoRelationService: AutoRelationService,
    private val relationEventService: RelationEventService,
    private val consumerConfiguration: ConsumerConfiguration,
    private val syncTrackerService: SyncTrackerService,
    private val resourceLockService: ResourceLockService,
) {
    fun processEntityConsumerRecord(record: EntityConsumerRecord) {
        resourceLockService.withLock(record.resourceName, record.key) {
            if (record.resource == null) {
                deleteEntity(record)
            } else {
                addToCache(record)
            }
        }

        if (record.type != null) {
            syncTrackerService.processRecordMetadata(record)
        }
    }

    private fun deleteEntity(record: EntityConsumerRecord) {
        val cache = cacheService.getCache(record.resourceName)

        cache
            .get(record.key)
            ?.let { relationEventService.removeRelations(record.resourceName, record.key, it) }

        cache.remove(record.key, record.timestamp)
    }

    private fun addToCache(record: EntityConsumerRecord) {
        val resource = requireNotNull(record.resource)
        val cache = cacheService.getCache(record.resourceName)

        if (consumerConfiguration.autorelation) {
            // reconcileLinks calls LinkService.mapLinks
            autoRelationService.reconcileLinks(record.resourceName, record.key, resource)
        }
        linkService.mapLinks(record.resourceName, resource)

        cache.put(record.key, resource, record.timestamp)
    }
}
