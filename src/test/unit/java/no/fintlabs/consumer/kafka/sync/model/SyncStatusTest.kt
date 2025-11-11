package no.fintlabs.consumer.kafka.sync.model

import com.github.benmanes.caffeine.cache.RemovalCause
import no.fintlabs.adapter.models.sync.SyncType
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SyncStatusTest {
    @Test
    fun `Removal cause is mapped correctly (COMPLETED)`() {
        val syncStatus = createSyncStatus("123", SyncType.FULL, RemovalCause.EXPLICIT)

        assertEquals(Status.COMPLETED, syncStatus.status)
    }

    @Test
    fun `Removal cause is mapped correctly (PARALLEL_FULL_SYNC)`() {
        val syncStatus = createSyncStatus("123", SyncType.FULL, RemovalCause.REPLACED)

        assertEquals(Status.PARALLEL_FULL_SYNC, syncStatus.status)
    }

    @Test
    fun `Removal cause is mapped correctly (EXPIRED)`() {
        val syncStatus = createSyncStatus("123", SyncType.FULL, RemovalCause.EXPIRED)

        assertEquals(Status.EXPIRED, syncStatus.status)
    }
}
