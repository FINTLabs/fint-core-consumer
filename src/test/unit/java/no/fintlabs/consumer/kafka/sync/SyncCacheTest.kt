package no.fintlabs.consumer.kafka.sync

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.mockk.junit5.MockKExtension
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.entity.EntitySync
import no.fintlabs.consumer.kafka.sync.model.SyncKey
import no.fintlabs.consumer.kafka.sync.model.SyncPhase
import no.fintlabs.consumer.kafka.sync.model.SyncState
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DynamicContainer
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID
import kotlin.test.assertEquals

@ExtendWith(MockKExtension::class)
class SyncCacheTest {
    private lateinit var cache: Cache<SyncKey, SyncState>
    private lateinit var sut: SyncCache
    private val resource = "elevfravar"

    @BeforeEach
    fun setUp() {
        cache = Caffeine.newBuilder().build()
        sut = SyncCache(cache)
    }

    /**
     * Shared, non-parallel scenarios verified across all SyncTypes: FULL, DELTA, DELETE.
     * Parallel scenarios are *only* tested for FULL (see Nested class below).
     */
    @TestFactory
    fun `non-parallel scenarios for all sync types`() = listOf(SyncType.FULL, SyncType.DELTA, SyncType.DELETE).map { type ->
        DynamicContainer.dynamicContainer(
            "SyncType=$type", listOf(
            DynamicTest.dynamicTest("size=1 completes immediately") {
                val sync = createSync(type = type, totalSize = 1)
                doSyncThenAssert(SyncPhase.COMPLETED, sync)
            },

            DynamicTest.dynamicTest("new sync with size>1 starts") {
                val sync = createSync(type = type, totalSize = 10)
                doSyncThenAssert(SyncPhase.STARTED, sync)
            },

            DynamicTest.dynamicTest("existing sync gets incremented") {
                val sync = createSync(type = type, totalSize = 10)
                doSyncThenAssert(SyncPhase.STARTED, sync)      // 1/10
                doSyncThenAssert(SyncPhase.INCREMENTED, sync)  // 2/10
            },

            DynamicTest.dynamicTest("finishing a sync returns completed (2 items)") {
                val sync = createSync(type = type, totalSize = 2)
                doSyncThenAssert(SyncPhase.STARTED, sync)      // 1/2
                doSyncThenAssert(SyncPhase.COMPLETED, sync)    // 2/2
            },

            DynamicTest.dynamicTest("finishing a sync returns completed (3 items with all phases)") {
                val sync = createSync(type = type, totalSize = 3)
                doSyncThenAssert(SyncPhase.STARTED, sync)       // 1/3
                doSyncThenAssert(SyncPhase.INCREMENTED, sync)   // 2/3
                doSyncThenAssert(SyncPhase.COMPLETED, sync)     // 3/3
            },

            DynamicTest.dynamicTest("completed sync gets removed from cache (fresh run starts again)") {
                val sync = createSync(type = type, totalSize = 3)
                doSyncThenAssert(SyncPhase.STARTED, sync)       // 1/3
                doSyncThenAssert(SyncPhase.INCREMENTED, sync)   // 2/3
                doSyncThenAssert(SyncPhase.COMPLETED, sync)     // 3/3 -> removal
                // New run with same correlation id should start fresh
                doSyncThenAssert(SyncPhase.STARTED, sync)
            },

            DynamicTest.dynamicTest("reject when same corrId mixes different totalSize") {
                val corr = UUID.randomUUID().toString()
                val first = createSync(type = type, corrId = corr, totalSize = 5)
                val second = createSync(type = type, corrId = corr, totalSize = 7) // different size

                doSyncThenAssert(SyncPhase.STARTED, first)      // 1/5
                // Same corrId but different declared size should be rejected by implementation policy
                doSyncThenAssert(SyncPhase.REJECTED, second)
            }
        ))
    }

    /**
     * FULL-only behaviors (parallel semantics etc.).
     */
    @Nested
    inner class FullSyncsOnly {

        @Test
        fun `overriding an existing full-sync returns started`() {
            val firstSync = createSync(type = SyncType.FULL)
            val secondSync = createSync(type = SyncType.FULL)

            doSyncThenAssert(SyncPhase.STARTED, firstSync)
            doSyncThenAssert(SyncPhase.STARTED, secondSync)
        }

        @Test
        fun `parallel full-syncs get rejected`() {
            val firstSync = createSync(type = SyncType.FULL)
            val secondSync = createSync(type = SyncType.FULL)
            val thirdSync = createSync(type = SyncType.FULL)

            // First sync is valid
            doSyncThenAssert(SyncPhase.STARTED, firstSync)

            // Second sync is valid (invalidates previous sync-id)
            doSyncThenAssert(SyncPhase.STARTED, secondSync)

            // First and second sync should now both be invalidated - due to running in parallel
            doSyncThenAssert(SyncPhase.REJECTED, firstSync)

            // Second invalidated id gets REJECTED
            doSyncThenAssert(SyncPhase.REJECTED, secondSync)

            // Start completely new sync
            doSyncThenAssert(SyncPhase.STARTED, thirdSync)

            // Verify all id-s are rejected due to being ran parallel
            doSyncThenAssert(SyncPhase.REJECTED, firstSync)
            doSyncThenAssert(SyncPhase.REJECTED, secondSync)
            doSyncThenAssert(SyncPhase.REJECTED, thirdSync)
        }

        @Test
        fun `parallel full-syncs for different resources are independent`() {
            val firstSync = createSync(type = SyncType.FULL)
            val secondSync = createSync(type = SyncType.FULL)
            val thirdSync = createSync(type = SyncType.FULL)

            val firstResource = "elevfravar"
            val secondResource = "elev"
            val thirdResource = "fravar"

            doSyncThenAssert(SyncPhase.STARTED, firstSync, firstResource)
            doSyncThenAssert(SyncPhase.STARTED, secondSync, secondResource)
            doSyncThenAssert(SyncPhase.INCREMENTED, firstSync, firstResource)
            doSyncThenAssert(SyncPhase.INCREMENTED, secondSync, secondResource)
            doSyncThenAssert(SyncPhase.STARTED, thirdSync, thirdResource)
            doSyncThenAssert(SyncPhase.INCREMENTED, thirdSync, thirdResource)
        }
    }

    /**
     * Asserts that the sync-phase after the given sync is as expected.
     */
    private fun doSyncThenAssert(expectedPhase: SyncPhase, sync: EntitySync, resource: String = this.resource) =
        assertEquals(expectedPhase, sut.recordSyncEvent(resource, sync))

    private fun createSync(
        type: SyncType = SyncType.FULL,
        corrId: String = UUID.randomUUID().toString(),
        totalSize: Long = 10L,
    ) = EntitySync(
        type = type,
        corrId = corrId,
        totalSize = totalSize,
    )
}