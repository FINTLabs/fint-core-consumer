package no.fintlabs.consumer.kafka.sync

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheEvictionService
import no.fintlabs.consumer.config.CaffeineCacheProperties
import no.fintlabs.consumer.kafka.entity.EntityConsumerRecord
import no.fintlabs.consumer.kafka.sync.SyncState.Completed
import no.fintlabs.consumer.kafka.sync.SyncState.ConcurrentFullSync
import no.fintlabs.consumer.kafka.sync.SyncState.Failed
import no.fintlabs.consumer.kafka.sync.SyncState.Init
import no.fintlabs.consumer.kafka.sync.SyncState.ResourceNameChanged
import no.fintlabs.consumer.kafka.sync.SyncState.TotalSizeChanged
import no.fintlabs.consumer.kafka.sync.model.SyncStatus
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * Service that tracks synchronization events for a specific resource.
 *
 * This service records the progress of a sync
 * and performs cache eviction when a [SyncType.FULL] synchronization has completed successfully.
 *
 * It uses a cache to store sync progress and [CacheEvictionService] to trigger
 * eviction when the sync is done.
 *
 * Concurrent FULL syncs -> All of them shall be tracked as failed and not trigger full sync
 * Changes in resource name or total size for a correlation ID -> Mark and report sync as failed. Do not report on following reports
 *
 */
@Service
class SyncTrackerService(
    private val evictionService: CacheEvictionService,
    private val syncStatusProducer: SyncStatusProducer,
    caffeineCacheProperties: CaffeineCacheProperties,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val fullSyncPerResourceName: MutableMap<String, Pair<String, SyncState>> = mutableMapOf()

    private val syncCache: Cache<String, SyncState> =
        Caffeine
            .newBuilder()
            .expireAfterAccess(caffeineCacheProperties.expireAfterAccess)
            .removalListener { correlationId: String?, state: SyncState?, cause: RemovalCause ->
                if (correlationId != null && state != null) {
                    if (cause == RemovalCause.EXPIRED) {
                        syncStatusProducer.publish(SyncStatus(correlationId, state.syncType, "Expired"))
                        logger.debug("Expired sync state {} with correlationId {} from cache", state, correlationId)
                    } else {
                        logger.trace(
                            "Sync state {} with correlationId {} was removed from cache because of {}",
                            state,
                            correlationId,
                            cause,
                        )
                    }
                } else {
                    logger.error(
                        "Detected unexpected sync-cache entry: correlationId {} removed because of {}",
                        correlationId,
                        cause,
                    )
                }
            }.build()

    /**
     * Update and process synchronization status for a resource through sync-metadata
     * received with a resources. Cache eviction is triggered on completion of full-syncs.
     * Other sync state changes are only logged and reported to the status service.
     *
     * @param consumerRecord the sync event details, including type and progress
     */
    fun processRecordMetadata(consumerRecord: EntityConsumerRecord) {
        val resourceName = consumerRecord.resourceName
        val correlationId = consumerRecord.corrId ?: throw IllegalStateException("No correlation id provided")
        val syncType = consumerRecord.type ?: throw IllegalStateException("No sync-type provided")
        val totalSize = consumerRecord.totalSize ?: throw IllegalStateException("Total size provided")
        val timestamp = consumerRecord.timestamp
        val previousSyncState = syncCache.get(correlationId) { Init(resourceName, totalSize, syncType) }!!
        val newSyncState = previousSyncState.transition(resourceName, timestamp, totalSize)

        // Check if there are other existing full-syncs for the same resource
        if (syncType == SyncType.FULL && newSyncState !is Failed) {
            val existingFullSync = fullSyncPerResourceName.put(resourceName, Pair(correlationId, newSyncState))
            if (existingFullSync != null && existingFullSync.first != correlationId) {
                // Fail existing ongoing full-sync on the same resource as a new received full-sync
                val (existingCorrelationID, existingSyncState) = existingFullSync
                val newStateForExistingFullSync =
                    ConcurrentFullSync(
                        existingSyncState.resourceName,
                        existingSyncState.startTimestamp,
                        existingSyncState.totalSize,
                        existingSyncState.processedCount,
                        existingSyncState.syncType,
                    )
                syncCache.put(existingCorrelationID, newStateForExistingFullSync)
                syncStatusProducer.publish(
                    SyncStatus(
                        existingCorrelationID,
                        SyncType.FULL,
                        newStateForExistingFullSync.description,
                    ),
                )
            }
        }

        if (newSyncState is Completed) {
            // Untrack completed syncs and evict completed full-syncs
            syncCache.invalidate(correlationId)
            logger.debug(
                "Completed {} sync with correlation ID {} and {} resources",
                newSyncState.syncType,
                correlationId,
                newSyncState.processedCount,
            )
            if (newSyncState.syncType == SyncType.FULL) {
                evictionService.evictExpired(resourceName, newSyncState.startTimestamp)
                fullSyncPerResourceName.remove(resourceName)
                syncStatusProducer.publish(SyncStatus(correlationId, newSyncState.syncType, "Completed"))
            }
        } else {
            syncCache.put(correlationId, newSyncState)
            if (newSyncState is ResourceNameChanged) {
                syncStatusProducer.publish(SyncStatus(correlationId, newSyncState.syncType, newSyncState.description))
            } else if (newSyncState is TotalSizeChanged) {
                syncStatusProducer.publish(SyncStatus(correlationId, newSyncState.syncType, newSyncState.description))
            }
        }
    }
}
