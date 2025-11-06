package no.fintlabs.consumer.kafka.sync

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.cache.CacheEvictionService
import no.fintlabs.consumer.kafka.entity.EntitySync
import no.fintlabs.consumer.kafka.sync.model.SyncPhase
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SyncTrackerServiceTest {

    private lateinit var evictionService: CacheEvictionService
    private lateinit var syncCache: SyncCache
    private lateinit var entitySync: EntitySync
    private lateinit var sut: SyncTrackerService
    private val resource = "elevfravar"

    @BeforeEach
    fun setUp() {
        evictionService = mockk(relaxed = true)
        syncCache = mockk()
        entitySync = mockk()
        sut = SyncTrackerService(syncCache, evictionService)
    }

    @Test
    fun `verify eviction is called on completion`() {
        every { syncCache.recordSyncEvent(resource, entitySync) } returns SyncPhase.COMPLETED

        sut.recordSync(resource, entitySync)

        verify(exactly = 1) { evictionService.triggerEviction(resource) }
    }

}