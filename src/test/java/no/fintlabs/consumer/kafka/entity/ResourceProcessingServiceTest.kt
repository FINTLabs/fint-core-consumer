package no.fintlabs.consumer.kafka.entity

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.RelationEventService
import no.fintlabs.cache.CacheService
import no.fintlabs.cache.FintCache
import no.fintlabs.consumer.config.AutorelationConfig
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.OrgId
import no.fintlabs.consumer.kafka.sync.SyncTrackerService
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceLockService
import no.novari.fint.model.resource.FintResource
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ResourceProcessingServiceTest {
    private val linkService = mockk<LinkService>(relaxed = true)
    private val cacheService = mockk<CacheService>()
    private val autoRelationService = mockk<AutoRelationService>(relaxed = true)
    private val relationEventService = mockk<RelationEventService>(relaxed = true)
    private val consumerConfiguration = mockk<ConsumerConfiguration>()
    private val syncTrackerService = mockk<SyncTrackerService>(relaxed = true)
    private val cache = mockk<FintCache<FintResource>>(relaxed = true)
    private val meterRegistry = SimpleMeterRegistry()
    private var resourceLockService: ResourceLockService =
        mockk {
            every { withLock(any(), any(), any()) } answers {
                val block = thirdArg<() -> Unit>()
                block()
            }
        }

    private lateinit var service: ResourceProcessingService

    @BeforeEach
    fun setup() {
        service =
            ResourceProcessingService(
                linkService,
                cacheService,
                autoRelationService,
                relationEventService,
                consumerConfiguration,
                syncTrackerService,
                meterRegistry,
                resourceLockService,
            )
        every { cacheService.getCache(any()) } returns cache
        every { consumerConfiguration.orgId } returns OrgId.from("org-123")
        every { consumerConfiguration.autorelation } returns AutorelationConfig(enabled = false)
    }

    @Test
    fun `null resource triggers delete path`() {
        val record = recordWith(resource = null, syncMetadata = null)
        every { cache.get(any()) } returns null

        service.processResourceMessage(record)

        verify { cache.remove(any(), any()) }
        verify(exactly = 0) { cache.put(any(), any(), any()) }
    }

    @Test
    fun `non-null resource triggers add to cache`() {
        val resource = mockk<FintResource>()
        val record = recordWith(resource = resource, syncMetadata = null)

        service.processResourceMessage(record)

        verify { cache.put(any(), resource, any()) }
        verify(exactly = 0) { cache.remove(any(), any()) }
    }

    @Test
    fun `delete removes relations when cache entry exists`() {
        val existing = mockk<FintResource>()
        val record = recordWith(resource = null, syncMetadata = null)
        every { cache.get(record.resourceId) } returns existing

        service.processResourceMessage(record)

        verify { relationEventService.removeRelations(record.resourceName, record.resourceId, existing) }
    }

    @Test
    fun `delete skips removeRelations when cache entry is absent`() {
        val record = recordWith(resource = null, syncMetadata = null)
        every { cache.get(any()) } returns null

        service.processResourceMessage(record)

        verify(exactly = 0) { relationEventService.removeRelations(any(), any(), any()) }
    }

    @Test
    fun `non-null syncMetadata triggers syncTrackerService`() {
        val syncMetadata = SyncMetadata(corrId = "corr-id", syncType = SyncType.FULL, totalSize = 10L)
        val record = recordWith(resource = mockk(), syncMetadata = syncMetadata)

        service.processResourceMessage(record)

        verify { syncTrackerService.processRecordMetadata(record.resourceName, syncMetadata, record.timestamp) }
    }

    @Test
    fun `null syncMetadata skips syncTrackerService`() {
        val record = recordWith(resource = mockk(), syncMetadata = null)

        service.processResourceMessage(record)

        verify(exactly = 0) { syncTrackerService.processRecordMetadata(any(), any(), any()) }
    }

    @Test
    fun `autorelation enabled calls mapLinks and reconcileLinks`() {
        every { consumerConfiguration.autorelation } returns AutorelationConfig(enabled = true)
        val resource = mockk<FintResource>()
        val record = recordWith(resource = resource, syncMetadata = null)

        service.processResourceMessage(record)

        verify(exactly = 1) { linkService.mapLinks(record.resourceName, record.resource) }
        verify(exactly = 1) { autoRelationService.reconcileLinks(record.resourceName, record.resourceId, resource) }
    }

    @Test
    fun `autorelation disabled calls mapLinks and skips reconcileLinks`() {
        val resource = mockk<FintResource>()
        val record = recordWith(resource = resource, syncMetadata = null)

        service.processResourceMessage(record)

        verify { linkService.mapLinks(record.resourceName, resource) }
        verify(exactly = 0) { autoRelationService.reconcileLinks(any(), any(), any()) }
    }

    @Test
    fun `records develop metrics and new lock metric for add path`() {
        val resource = mockk<FintResource>()
        val syncMetadata = SyncMetadata(corrId = "corr-id", syncType = SyncType.FULL, totalSize = 10L)
        val record = recordWith(resource = resource, syncMetadata = syncMetadata)

        service.processResourceMessage(record)

        verifyTimer("record.process.total")
        verifyTimer("record.addPath")
        verifyTimer("cache.getCache")
        verifyTimer("links.map")
        verifyTimer("cache.put")
        verifyTimer("sync.processRecordMetadata")
    }

    private fun recordWith(
        resource: FintResource?,
        syncMetadata: SyncMetadata?,
    ): ResourceMessage =
        ResourceMessage(
            resourceName = "test-resource",
            resourceId = "test-key",
            resource = resource,
            timestamp = 1000L,
            syncMetadata = syncMetadata,
        )

    private fun verifyTimer(operation: String) {
        val timers = meterRegistry.find("core.consumer.processing").tag("operation", operation).timers()

        check(timers.isNotEmpty()) { "Expected timer for operation $operation" }
        kotlin.test.assertEquals(1, timers.sumOf { it.count() })
    }
}
