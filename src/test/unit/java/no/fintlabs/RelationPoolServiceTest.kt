package no.fintlabs

import io.mockk.*
import no.fintlabs.autorelation.model.RelationRef
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.ResourceRef
import no.fintlabs.consumer.config.RelationPoolConfig
import no.fintlabs.consumer.kafka.event.RelationUpdateDlqProducer
import no.fintlabs.consumer.links.RelationPoolService
import no.fintlabs.consumer.links.RelationService
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class RelationPoolServiceTest {

    private val relationService: RelationService = mockk()
    private val dlqProducer: RelationUpdateDlqProducer = mockk()
    private val relationUpdate: RelationUpdate = mockk(relaxed = true)

    private lateinit var service: RelationPoolService

    @BeforeEach
    fun setUp() {
        val resource = mockk<ResourceRef> {
            every { name } returns "resA"
        }
        val relation = mockk<RelationRef> {
            every { name } returns "relX"
        }

        every { relationUpdate.resource } returns resource
        every { relationUpdate.relation } returns relation

        service = RelationPoolService(
            poolConfig = createTestConfig(),
            relationService = relationService,
            dlqProducer = dlqProducer
        )

        service.start()
    }

    @AfterEach
    fun tearDown() {
        service.stop()
        clearAllMocks()
    }

    @Test
    fun `successful processing stops after first attempt`() {
        every { relationService.processRelationUpdate(any()) } returns true

        service.enqueue(relationUpdate)

        await().atMost(2, TimeUnit.SECONDS).untilAsserted {
            verify(exactly = 1) { relationService.processRelationUpdate(relationUpdate) }
            verify(exactly = 0) { dlqProducer.publish(any()) }
        }
    }

    @Test
    fun `failing maxAttempts times sends to DLQ`() {
        every { relationService.processRelationUpdate(any()) } returns false

        service.enqueue(relationUpdate)

        await().atMost(3, TimeUnit.SECONDS).untilAsserted {
            verify(exactly = 3) { relationService.processRelationUpdate(relationUpdate) }
            verify(exactly = 1) { dlqProducer.publish(relationUpdate) }
        }
    }

    @Test
    fun `listener enqueues update`() {
        every { relationService.processRelationUpdate(any()) } returns true

        service.onEnqueued(relationUpdate)

        await().atMost(2, TimeUnit.SECONDS).untilAsserted {
            verify(exactly = 1) { relationService.processRelationUpdate(relationUpdate) }
            verify(exactly = 0) { dlqProducer.publish(any()) }
        }
    }

    private fun createTestConfig() = RelationPoolConfig().apply {
        maxAttempts = 3
        initialDelaySeconds = 0
        exponentialBackoff = false
        backoffMultiplier = 2.0
    }
}