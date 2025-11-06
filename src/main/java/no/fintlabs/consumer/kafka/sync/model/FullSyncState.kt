package no.fintlabs.consumer.kafka.sync.model

import no.fintlabs.consumer.kafka.entity.EntitySync

class FullSyncState(
    sync: EntitySync,
) : SyncState(sync.corrId, sync.totalSize) {
    val previousSessionIds: MutableSet<String> = LinkedHashSet()
}

fun FullSyncState.newSync(sync: EntitySync) =
    FullSyncState(sync).apply {
        previousSessionIds.add(this@newSync.corrId)
        previousSessionIds.addAll(this@newSync.previousSessionIds)
        invalid = previousSessionIds.contains(sync.corrId)
    }
