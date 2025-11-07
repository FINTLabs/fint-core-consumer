package no.fintlabs.consumer.kafka.sync.model

import no.fintlabs.adapter.models.sync.SyncType

enum class SyncPhase {
    STARTED,
    INCREMENTED,
    COMPLETED,
    REJECTED,
    ;

    fun completedFullSync(syncType: SyncType) = this == COMPLETED && syncType == SyncType.FULL
}

fun derivePhase(syncState: SyncState): SyncPhase =
    when {
        syncState.invalid -> SyncPhase.REJECTED
        syncState.currentCount == syncState.totalSize -> SyncPhase.COMPLETED
        syncState.currentCount == 1L -> SyncPhase.STARTED
        syncState.currentCount < syncState.totalSize -> SyncPhase.INCREMENTED
        else -> SyncPhase.REJECTED
    }
