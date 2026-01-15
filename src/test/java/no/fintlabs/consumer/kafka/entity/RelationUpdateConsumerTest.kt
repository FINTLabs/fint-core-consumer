package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.relation.RelationService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RelationUpdateConsumerTest {

    private lateinit var relationService: RelationService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var relationUpdateConsumer: RelationUpdateConsumer
    private lateinit var consumerRecord: ConsumerRecord<String, RelationUpdate>
    private lateinit var relationUpdate: RelationUpdate

    @BeforeEach
    fun setUp() {
        relationService = mockk(relaxed = true)
        consumerConfig = mockk()
        relationUpdate = mockk()
        consumerRecord = mockk {
            every { value() } returns relationUpdate
        }

        relationUpdateConsumer = RelationUpdateConsumer(relationService, consumerConfig)
    }

    @Test
    fun `process if consumerConfiguration matches`() {
        val domain = "testdomain"
        val pkg = "pkgtest"
        val orgId = "orgId"

        every { relationUpdate.targetEntity.domainName } returns domain
        every { relationUpdate.targetEntity.packageName } returns pkg
        every { relationUpdate.orgId } returns orgId

        every { consumerConfig.matchesConfiguration(domain, pkg, orgId) } returns true

        relationUpdateConsumer.consumeRecord(consumerRecord)

        verify(exactly = 1) { relationService.applyOrBufferUpdate(any()) }
    }

}