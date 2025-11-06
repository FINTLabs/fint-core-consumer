package no.fintlabs.consumer.kafka.sync.model

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.entity.EntitySync
import java.util.concurrent.atomic.AtomicLong

open class SyncState(
    val corrId: String,
    val totalSize: Long,
    val progress: AtomicLong = AtomicLong(0),
    var invalid: Boolean = false,
) {
    fun incrementProgress(): SyncState = this.apply { progress.incrementAndGet() }

    fun invalidate(): SyncState = this.apply { invalid = true }

    fun totalSizeMismatch(sync: EntitySync): Boolean = this.totalSize != sync.totalSize
}

fun createSyncState(sync: EntitySync): SyncState =
    when (sync.type) {
        SyncType.FULL -> FullSyncState(sync)
        else -> SyncState(sync.corrId, sync.totalSize)
    }
