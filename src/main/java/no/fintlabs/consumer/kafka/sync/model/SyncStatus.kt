package no.fintlabs.consumer.kafka.sync.model

import com.github.benmanes.caffeine.cache.RemovalCause
import no.fintlabs.adapter.models.sync.SyncType

/**
 * Represents the status of a synchronization process.
 *
 * <p>This class is used to send messages about successful or failed sync operations
 * based on their completion status. Each {@code SyncStatus} instance includes a
 * correlation ID, a synchronization type, and a resulting status.</p>
 *
 * @property corrId A unique identifier for correlating sync messages.
 * @property type The type of synchronization (defined by {@link SyncType}).
 * @property status The outcome status of the sync process.
 */
data class SyncStatus(
    val corrId: String,
    val type: SyncType,
    val status: Status,
)

/**
 * Creates a {@link SyncStatus} instance based on the provided {@link RemovalCause}.
 *
 * @param corrId The correlation ID associated with the sync.
 * @param type The synchronization type.
 * @param cause The reason for removal from the cache, used to determine the sync status.
 * @return A new {@code SyncStatus} object with an appropriate {@link Status}.
 */
fun createSyncStatus(
    corrId: String,
    type: SyncType,
    cause: RemovalCause,
) = SyncStatus(
    corrId = corrId,
    type = type,
    status = createStatus(cause),
)

enum class Status {
    COMPLETED,
    EXPIRED,
    PARALLEL_FULL_SYNC,
}

fun createStatus(cause: RemovalCause) =
    when (cause) {
        RemovalCause.EXPIRED -> Status.EXPIRED
        RemovalCause.REPLACED -> Status.PARALLEL_FULL_SYNC
        RemovalCause.EXPLICIT -> Status.COMPLETED
        else -> Status.EXPIRED
    }
