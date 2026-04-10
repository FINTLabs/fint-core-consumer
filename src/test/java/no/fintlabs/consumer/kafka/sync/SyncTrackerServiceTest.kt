package no.fintlabs.consumer.kafka.sync

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.Called
import io.mockk.clearAllMocks
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifySequence
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheEvictionService
import no.fintlabs.consumer.config.CaffeineCacheProperties
import no.fintlabs.consumer.kafka.entity.SyncMetadata
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertContains
import kotlin.test.assertEquals

class SyncTrackerServiceTest {
    private lateinit var evictionService: CacheEvictionService
    private lateinit var syncStatusProducer: SyncStatusProducer
    private lateinit var syncTracker: SyncTrackerService
    private val meterRegistry = SimpleMeterRegistry()
    private val cacheProperties: CaffeineCacheProperties = CaffeineCacheProperties()
    private val resourceName = "elevfravar"

    @BeforeEach
    fun setUp() {
        evictionService = mockk(relaxed = true)
        syncStatusProducer = mockk(relaxed = true)
        syncTracker = SyncTrackerService(evictionService, syncStatusProducer, meterRegistry, cacheProperties)
    }

    @Test
    fun `full-sync with one record and total size 1 shall trigger eviction and send sync-status`() {
        val correlationId = "test-corr-id"
        val timestamp = System.currentTimeMillis()
        syncTracker.processRecordMetadata(
            resourceName,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 1),
            timestamp,
        )

        verify(exactly = 1) { evictionService.evictExpired(resourceName, timestamp) }
        verify(exactly = 1) {
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationId, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains("Completed", it.status)
                },
            )
        }
    }

    /**
     * Interval for full-syncs is set in days. Full-syncs for the same resource shall therefore never
     * happen concurrently. Concurrent full-syncs for the same resource likely either means two adapters
     * synchronizes the same resource or that there is an internal bug in an adapter. If this situation
     * occurs, the first full-sync is marked as failed and reported to the sync status service. The last
     * full-sync is continued.
     */
    @Test
    fun `overlapping full-syncs for same resource shall fail the existing full-sync`() {
        val correlationIdA = "corr-id-A"
        val correlationIdB = "corr-id-B"

        // Process first of two records of sync A => Start tracking of sync A
        syncTracker.processRecordMetadata(
            resourceName,
            SyncMetadata(corrId = correlationIdA, syncType = SyncType.FULL, totalSize = 2),
            1234,
        )
        verify { evictionService wasNot Called }
        clearAllMocks()

        // Process first of two records of sync B => Fail sync A and publish failure. Start tracking of sync B
        syncTracker.processRecordMetadata(
            resourceName,
            SyncMetadata(corrId = correlationIdB, syncType = SyncType.FULL, totalSize = 2),
            1234,
        )
        verify { evictionService wasNot Called }
        verify {
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationIdA, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains("Concurrent full-sync of $resourceName resource", it.status)
                },
            )
        }
        clearAllMocks()

        // Process last of two records of sync B => Complete sync B and publish complete full-sync
        syncTracker.processRecordMetadata(
            resourceName,
            SyncMetadata(corrId = correlationIdB, syncType = SyncType.FULL, totalSize = 2),
            1235,
        )
        verify(exactly = 1) {
            evictionService.evictExpired(
                resourceName,
                1234,
            )
        } // Timestamp of earliest record for sync B
        verify(exactly = 1) {
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationIdB, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains("Completed", it.status)
                },
            )
        }
        clearAllMocks()

        // Process last of two records of sync A => Do nothing. Will be removed by expiry
        syncTracker.processRecordMetadata(
            resourceName,
            SyncMetadata(corrId = correlationIdA, syncType = SyncType.FULL, totalSize = 2),
            1234,
        )
        verify { evictionService wasNot Called }
        verify { syncStatusProducer wasNot Called }
    }

    @Test
    fun `change of resource name for a ongoing sync shall fail that sync`() {
        val correlationId = "corr-id-A"

        // Process first of three records
        syncTracker.processRecordMetadata(
            resourceName,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            4,
        )
        verify { evictionService wasNot Called }
        verify { syncStatusProducer wasNot Called }
        clearAllMocks()

        // Process second of three records, but with another resource name => Fail sync and publish failure
        syncTracker.processRecordMetadata(
            "another-resource-name",
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            5,
        )
        verify { evictionService wasNot Called }
        verify {
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationId, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains(it.status, "Resource name changed")
                },
            )
        }
        clearAllMocks()

        // Process last of three records => Sync is failed. Do nothing
        syncTracker.processRecordMetadata(
            "another-resource-name",
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            6,
        )
        verify { evictionService wasNot Called }
        verify { syncStatusProducer wasNot Called }
    }

    @Test
    fun `full-syncs for different resource can be interleaved`() {
        val correlationIdA = "corr-id-A"
        val correlationIdB = "corr-id-B"
        val correlationIdC = "corr-id-C"

        val resourceNameA = "resource-name-A"
        val resourceNameB = "resource-name-B"
        val resourceNameC = "resource-name-C"

        // Process first of three records
        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationIdA, syncType = SyncType.FULL, totalSize = 3),
            1,
        )
        syncTracker.processRecordMetadata(
            resourceNameB,
            SyncMetadata(corrId = correlationIdB, syncType = SyncType.FULL, totalSize = 3),
            1,
        )
        syncTracker.processRecordMetadata(
            resourceNameC,
            SyncMetadata(corrId = correlationIdC, syncType = SyncType.FULL, totalSize = 3),
            1,
        )
        verify { evictionService wasNot Called }
        verify { syncStatusProducer wasNot Called }
        clearAllMocks()

        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationIdA, syncType = SyncType.FULL, totalSize = 3),
            2,
        )
        syncTracker.processRecordMetadata(
            resourceNameB,
            SyncMetadata(corrId = correlationIdB, syncType = SyncType.FULL, totalSize = 3),
            2,
        )
        syncTracker.processRecordMetadata(
            resourceNameC,
            SyncMetadata(corrId = correlationIdC, syncType = SyncType.FULL, totalSize = 3),
            2,
        )
        verify { evictionService wasNot Called }
        verify { syncStatusProducer wasNot Called }
        clearAllMocks()

        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationIdA, syncType = SyncType.FULL, totalSize = 3),
            3,
        )
        syncTracker.processRecordMetadata(
            resourceNameB,
            SyncMetadata(corrId = correlationIdB, syncType = SyncType.FULL, totalSize = 3),
            3,
        )
        syncTracker.processRecordMetadata(
            resourceNameC,
            SyncMetadata(corrId = correlationIdC, syncType = SyncType.FULL, totalSize = 3),
            3,
        )

        verifySequence {
            evictionService.evictExpired(resourceNameA, 1)
            evictionService.evictExpired(resourceNameB, 1)
            evictionService.evictExpired(resourceNameC, 1)
        }
        verifySequence {
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationIdA, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains(it.status, "Completed")
                },
            )
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationIdB, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains(it.status, "Completed")
                },
            )
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationIdC, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains(it.status, "Completed")
                },
            )
        }
    }

    @Test
    fun `correlation id can be reused when not interleaved`() {
        val correlationId = "corr-id-A"

        val resourceNameA = "resource-name-A"
        val resourceNameB = "resource-name-B"

        // Process all three records of first full-sync
        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            1,
        )
        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            2,
        )
        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            3,
        )
        verify {
            evictionService.evictExpired(resourceNameA, 1)
        }
        verify {
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationId, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains(it.status, "Completed")
                },
            )
        }
        clearAllMocks()

        // Process all three records of a delta-sync
        syncTracker.processRecordMetadata(
            resourceNameB,
            SyncMetadata(corrId = correlationId, syncType = SyncType.DELTA, totalSize = 3),
            4,
        )
        syncTracker.processRecordMetadata(
            resourceNameB,
            SyncMetadata(corrId = correlationId, syncType = SyncType.DELTA, totalSize = 3),
            5,
        )
        syncTracker.processRecordMetadata(
            resourceNameB,
            SyncMetadata(corrId = correlationId, syncType = SyncType.DELTA, totalSize = 3),
            6,
        )
        verify { evictionService wasNot Called }
        verify { syncStatusProducer wasNot Called }
        clearAllMocks()

        // Process all three records of a full-sync
        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            7,
        )
        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            8,
        )
        syncTracker.processRecordMetadata(
            resourceNameA,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 3),
            9,
        )
        verify {
            evictionService.evictExpired(resourceNameA, 7)
        }
        verify {
            syncStatusProducer.produce(
                withArg {
                    assertEquals(correlationId, it.corrId)
                    assertEquals(SyncType.FULL, it.type)
                    assertContains(it.status, "Completed")
                },
            )
        }
    }

    @Test
    fun `records develop sync metrics and new lock metric for completed full sync`() {
        val correlationId = "metrics-corr-id"
        syncTracker.processRecordMetadata(
            resourceName,
            SyncMetadata(corrId = correlationId, syncType = SyncType.FULL, totalSize = 1),
            1234,
        )

        verifyTimer("sync.processRecordMetadata")
        verifyTimer("sync.state.load")
        verifyTimer("sync.state.transition")
        verifyTimer("sync.full.updateTracking")
        verifyTimer("sync.state.invalidate")
        verifyTimer("sync.full.evictExpired")
        verifyTimer("sync.full.removeTracking")
        verifyTimer("sync.status.publish.completed")
    }

    private fun verifyTimer(operation: String) {
        val timers = meterRegistry.find("core.consumer.sync.processing").tag("operation", operation).timers()

        check(timers.isNotEmpty()) { "Expected timer for operation $operation" }
        assertEquals(1, timers.sumOf { it.count() })
    }
}
