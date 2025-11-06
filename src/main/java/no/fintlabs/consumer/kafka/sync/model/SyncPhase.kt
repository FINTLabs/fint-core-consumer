package no.fintlabs.consumer.kafka.sync.model

enum class SyncPhase {
    STARTED,
    INCREMENTED,
    COMPLETED,
    REJECTED,
}

fun derivePhase(syncState: SyncState): SyncPhase =
    when {
        syncState.invalid -> SyncPhase.REJECTED
        syncState.currentCount == syncState.totalSize -> SyncPhase.COMPLETED
        syncState.currentCount == 1L -> SyncPhase.STARTED
        syncState.currentCount < syncState.totalSize -> SyncPhase.INCREMENTED
        else -> SyncPhase.REJECTED
    }
