package no.fintlabs.consumer.kafka.sync

import com.github.benmanes.caffeine.cache.Cache
import no.fintlabs.consumer.kafka.entity.EntitySync
import no.fintlabs.consumer.kafka.sync.model.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Maintains sync sessions in a cache.
 *
 * **Validity tracking** is only enforced for **FULL** syncs:
 * - At most one **uninterrupted** FULL sync may be active per resource.
 * - A conflicting/parallel FULL sync (e.g., new `corrId` or a known overlapping session)
 *   invalidates the existing FULL sync state and may replace it.
 *
 * **DELTA** and **DELETE** syncs are handled **loosely**:
 * - Multiple may be active concurrently for the same resource.
 * - They do not participate in validity checks; we simply track counts/progress until completion.
 */
@Component
class SyncCache(
    private val cache: Cache<SyncKey, SyncState>,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Records a sync event for the given resource and updates the current sync state.
     *
     * <p>If the event corresponds to an ongoing session, the state is updated and progress advanced.
     * If it completes, the state is removed from the cache.</p>
     *
     * @param resourceName the name of the resource being synchronized
     * @param event the sync event containing metadata and progress
     * @return the resulting sync phase, or {@link SyncPhase#REJECTED} if the state could not be determined or is invalid
     */
    fun recordSyncEvent(
        resourceName: String,
        event: EntitySync,
    ): SyncPhase =
        createSyncKey(resourceName, event).let { key ->
            upsertStateFor(key, event)
                ?.incrementProgress()
                ?.let { derivePhase(it) }
                ?.also { removeIfCompleted(key, it) }
                ?: run {
                    logger.warn("Failed to determine sync state for resource: $resourceName")
                    SyncPhase.REJECTED
                }
        }

    /**
     * Inserts or updates the cache entry for a sync session.
     *
     * <p>Handles both FULL and incremental syncs by validating and updating
     * the current {@link SyncState} accordingly.</p>
     *
     * @param cacheKey the cache key identifying the resource and sync type
     * @param event the sync event
     * @return the updated or newly created {@link SyncState}, or null if invalid
     */
    private fun upsertStateFor(
        cacheKey: SyncKey,
        event: EntitySync,
    ): SyncState? =
        cache
            .asMap()
            .compute(cacheKey) { _, existing ->
                when (existing) {
                    null -> createSyncState(event)
                    is FullSyncState -> updateFullStateIfNewSession(existing, event)
                    is SyncState -> validateTotalSize(existing, event)
                }
            }

    private fun validateTotalSize(
        existing: SyncState,
        event: EntitySync,
    ): SyncState =
        if (existing.totalSizeMismatch(event)) {
            existing.invalidate()
        } else {
            existing
        }

    private fun removeIfCompleted(
        key: SyncKey,
        phase: SyncPhase,
    ) = takeIf { phase == SyncPhase.COMPLETED }
        ?.run { cache.invalidate(key) }

    private fun updateFullStateIfNewSession(
        existing: FullSyncState,
        event: EntitySync,
    ): SyncState =
        if (isNewCorrelationId(existing, event)) {
            existing.newSync(event)
        } else {
            validateTotalSize(existing, event)
        }

    private fun isNewCorrelationId(
        existing: FullSyncState,
        event: EntitySync,
    ): Boolean = existing.corrId != event.corrId
}
