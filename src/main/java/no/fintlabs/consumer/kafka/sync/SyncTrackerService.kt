package no.fintlabs.consumer.kafka.sync

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import com.google.common.util.concurrent.Striped
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheEvictionService
import no.fintlabs.consumer.config.CaffeineCacheProperties
import no.fintlabs.consumer.kafka.entity.SyncMetadata
import no.fintlabs.consumer.kafka.sync.SyncState.Completed
import no.fintlabs.consumer.kafka.sync.SyncState.ConcurrentFullSync
import no.fintlabs.consumer.kafka.sync.SyncState.Failed
import no.fintlabs.consumer.kafka.sync.SyncState.Init
import no.fintlabs.consumer.kafka.sync.SyncState.ResourceNameChanged
import no.fintlabs.consumer.kafka.sync.SyncState.TotalSizeChanged
import no.fintlabs.consumer.kafka.sync.model.SyncStatus
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import kotlin.concurrent.withLock

/**
 * Service that tracks synchronization events for a specific resource.
 *
 * This service records the progress of a sync
 * and performs cache eviction when a [SyncType.FULL] synchronization has completed successfully.
 *
 * It uses a cache to store sync progress and [CacheEvictionService] to trigger
 * eviction when the sync is done.
 *
 * Concurrent FULL syncs -> All of them shall be tracked as failed and not trigger full sync.
 * Changes in resource name or total size for a correlation ID -> Mark and report sync as failed.
 *
 */
@Service
class SyncTrackerService(
    private val evictionService: CacheEvictionService,
    private val syncStatusProducer: SyncStatusProducer,
    private val meterRegistry: MeterRegistry,
    caffeineCacheProperties: CaffeineCacheProperties,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val resourceLocks = Striped.lazyWeakLock(32)
    private val fullSyncPerResourceName: MutableMap<String, Pair<String, SyncState>> = mutableMapOf()

    private val syncCache: Cache<String, SyncState> =
        Caffeine
            .newBuilder()
            .expireAfterAccess(caffeineCacheProperties.expireAfterAccess)
            .removalListener { correlationId: String?, state: SyncState?, cause: RemovalCause ->
                if (correlationId != null && state != null) {
                    if (cause == RemovalCause.EXPIRED) {
                        syncStatusProducer.produce(SyncStatus(correlationId, state.syncType, "Expired"))
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
     * received together with a resource. Cache eviction is triggered on completion of full-syncs.
     * Other sync state changes are only logged and reported to the status service.
     *
     * @param consumerRecord the sync event details, including type and progress
     */
    fun processRecordMetadata(
        resourceName: String,
        syncMetadata: SyncMetadata,
        timeStamp: Long,
    ) {
        timed(resourceName, syncMetadata.syncType, "sync.processRecordMetadata") {
            resourceLocks.get(resourceName).withLock {
                processRecordMetadataLocked(resourceName, syncMetadata, timeStamp)
            }
        }
    }

    private fun processRecordMetadataLocked(
        resourceName: String,
        syncMetadata: SyncMetadata,
        timestamp: Long,
    ) = with(syncMetadata) {
        val previousSyncState =
            timed(resourceName, syncType, "sync.state.load") {
                syncCache.get(corrId) { Init(resourceName, totalSize, syncType) }
            }
        val newSyncState =
            timed(resourceName, syncType, "sync.state.transition") {
                previousSyncState.transition(resourceName, timestamp, totalSize)
            }

        if (syncType == SyncType.FULL && newSyncState !is Failed) {
            timed(resourceName, syncType, "sync.full.updateTracking") {
                val existingFullSync = fullSyncPerResourceName.put(resourceName, Pair(corrId, newSyncState))
                if (existingFullSync != null && existingFullSync.first != corrId) {
                    logger.warn(
                        "Concurrent full sync detected: resource={}, existingCorrelationId={}, newCorrelationId={}",
                        resourceName,
                        existingFullSync.first,
                        corrId,
                    )
                    val (existingCorrelationID, existingSyncState) = existingFullSync
                    val newStateForExistingFullSync =
                        ConcurrentFullSync(
                            existingSyncState.resourceName,
                            existingSyncState.timestamp,
                            existingSyncState.totalSize,
                            existingSyncState.processedCount,
                            existingSyncState.syncType,
                        )
                    syncCache.put(existingCorrelationID, newStateForExistingFullSync)
                    timed(resourceName, syncType, "sync.status.publish.concurrentFullSync") {
                        syncStatusProducer.produce(
                            SyncStatus(
                                existingCorrelationID,
                                SyncType.FULL,
                                newStateForExistingFullSync.description,
                            ),
                        )
                    }
                }
            }
        }

        if (newSyncState is Completed) {
            timed(resourceName, syncType, "sync.state.invalidate") {
                syncCache.invalidate(corrId)
            }
            logger.debug(
                "Completed {} sync with correlation ID {} and {} resources",
                newSyncState.syncType,
                corrId,
                newSyncState.processedCount,
            )
            if (newSyncState.syncType == SyncType.FULL) {
                logger.info(
                    "Full sync completed, starting cache eviction: corrId={}, resource={}, processedCount={}",
                    corrId,
                    resourceName,
                    newSyncState.processedCount,
                )
                timed(resourceName, syncType, "sync.full.evictExpired") {
                    evictionService.evictExpired(resourceName, newSyncState.timestamp)
                }
                timed(resourceName, syncType, "sync.full.removeTracking") {
                    fullSyncPerResourceName.remove(resourceName)
                }
                timed(resourceName, syncType, "sync.status.publish.completed") {
                    syncStatusProducer.produce(SyncStatus(corrId, newSyncState.syncType, "Completed"))
                }
            }
        } else {
            timed(resourceName, syncType, "sync.state.store") {
                syncCache.put(corrId, newSyncState)
            }
            if (newSyncState is ResourceNameChanged) {
                logger.warn(
                    "Sync state validation failed: corrId={}, resource={}, reason={}",
                    corrId,
                    resourceName,
                    newSyncState.description,
                )
                timed(resourceName, syncType, "sync.status.publish.resourceNameChanged") {
                    syncStatusProducer.produce(
                        SyncStatus(corrId, newSyncState.syncType, newSyncState.description),
                    )
                }
            } else if (newSyncState is TotalSizeChanged) {
                logger.warn(
                    "Sync state validation failed: corrId={}, resource={}, reason={}",
                    corrId,
                    resourceName,
                    newSyncState.description,
                )
                timed(resourceName, syncType, "sync.status.publish.totalSizeChanged") {
                    syncStatusProducer.produce(
                        SyncStatus(corrId, newSyncState.syncType, newSyncState.description),
                    )
                }
            }
        }
    }

    private fun <T> timed(
        resourceName: String,
        syncType: SyncType,
        operation: String,
        supplier: () -> T,
    ): T {
        val sample = Timer.start(meterRegistry)
        var status = "success"
        return try {
            supplier.invoke()
        } catch (exception: RuntimeException) {
            status = "error"
            logger.error(
                "Sync component failed: operation={}, resource={}, syncType={}, status={}",
                operation,
                safeResourceName(resourceName),
                syncType,
                status,
                exception,
            )
            throw exception
        } finally {
            val duration = Duration.ofNanos(sample.stop(timer(resourceName, syncType, operation, status)))
            if (duration > SLOW_COMPONENT_THRESHOLD) {
                logger.warn(
                    "Slow sync component detected: operation={}, durationMs={}, resource={}, syncType={}, status={}",
                    operation,
                    duration.toMillis(),
                    safeResourceName(resourceName),
                    syncType,
                    status,
                )
            }
        }
    }

    private fun timer(
        resourceName: String,
        syncType: SyncType,
        operation: String,
        status: String,
    ): Timer =
        Timer
            .builder("core.consumer.sync.processing")
            .description("Duration of synchronization tracking and cache eviction processing")
            .tag("resource", safeResourceName(resourceName))
            .tag("sync_type", syncType.name.lowercase())
            .tag("operation", operation)
            .tag("status", status)
            .register(meterRegistry)

    private fun safeResourceName(resourceName: String?): String = resourceName?.takeIf { it.isNotBlank() } ?: "unknown"

    companion object {
        private val SLOW_COMPONENT_THRESHOLD = Duration.ofSeconds(10)
    }
}
