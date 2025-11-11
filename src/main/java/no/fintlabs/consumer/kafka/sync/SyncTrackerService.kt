package no.fintlabs.consumer.kafka.sync

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheEvictionService
import no.fintlabs.consumer.kafka.entity.EntitySync
import no.fintlabs.consumer.kafka.sync.model.SyncPhase
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * Service that tracks synchronization events for a specific resource.
 *
 * This service records the progress of a sync
 * and performs cache eviction when a [SyncType.FULL] synchronization has completed successfully.
 *
 * It uses [SyncCache] to store sync progress and [CacheEvictionService] to trigger
 * eviction when the sync is done.
 */
@Service
class SyncTrackerService(
    private val syncCache: SyncCache,
    private val evictionService: CacheEvictionService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Records a sync event for a given resource.
     *
     * This method updates the sync progress in the cache and checks if the
     * sync has completed. If the sync is a full sync and has finished,
     * it triggers a cache eviction for that resource.
     *
     * @param resource the resource identifier being synchronized
     * @param sync the sync event details, including type and progress
     */
    fun recordSync(
        resource: String,
        sync: EntitySync,
    ) = handleResult(resource, sync.type, syncCache.recordSyncEvent(resource, sync))

    private fun handleResult(
        resource: String,
        syncType: SyncType,
        phase: SyncPhase,
    ) = if (phase.completedFullSync(syncType)) {
        logger.info("Sync completed for resource $resource")
        evictionService.triggerEviction(resource)
    } else {
        logger.debug("Sync {} for resource {}", phase.name, resource)
    }
}
