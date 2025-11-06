package no.fintlabs.consumer.kafka.sync.model

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.entity.EntitySync

/**
 * Represents a unique key for tracking a sync session in the cache.
 *
 * @property resource The name of the resource being synchronized.
 * @property type The type of sync (FULL, DELTA, or DELETE).
 * @property corrId The correlation ID for the sync. This is used for
 * DELTA and DELETE syncs, since multiple syncs of these types may
 * be active concurrently for the same resource.
 *
 * For FULL syncs, this field is always `null` because only one full
 * sync per resource is allowed at a time.
 */
data class SyncKey(
    val resource: String,
    val type: SyncType,
    val corrId: String? = null,
)

/**
 * Creates a [SyncKey] from the given resource and [EntitySync].
 *
 * The correlation ID is included for DELTA and DELETE syncs, since
 * these types are uniquely identified by their correlation IDs.
 * FULL syncs omit the correlation ID to ensure that only one active
 * full sync per resource is allowed.
 */
fun createSyncKey(
    resource: String,
    sync: EntitySync,
) = SyncKey(
    resource = resource,
    type = sync.type,
    corrId = sync.corrId.takeIf { sync.type == SyncType.DELTA || sync.type == SyncType.DELETE },
)
