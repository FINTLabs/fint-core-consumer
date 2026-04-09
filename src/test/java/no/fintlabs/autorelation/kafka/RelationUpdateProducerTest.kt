package no.fintlabs.autorelation.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.createEntityDescriptor
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.OrgId
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.novari.fint.model.resource.Link
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import java.util.concurrent.CompletableFuture

class RelationUpdateProducerTest {
    private val kafkaTemplate = mockk<KafkaTemplate<String, RelationUpdate>>()
    private val consumerConfiguration = mockk<ConsumerConfiguration>()
    private val kafkaThroughputMetrics = mockk<KafkaThroughputMetrics>(relaxed = true)

    private lateinit var producer: RelationUpdateProducer

    @BeforeEach
    fun setUp() {
        every { consumerConfiguration.orgId } returns OrgId.from("foo.bar")
        every {
            kafkaTemplate.send(
                any<String>(),
                any(),
                any<RelationUpdate>(),
            )
        } returns CompletableFuture.completedFuture(mockk())
        producer = RelationUpdateProducer(kafkaTemplate, consumerConfiguration, kafkaThroughputMetrics)
    }

    @Test
    fun `produce sends to correct topic`() {
        val relationUpdate = createRelationUpdate("utdanning", "vurdering", "elevfravar")

        producer.produce(relationUpdate, "elev", "abc123")

        verify {
            kafkaTemplate.send(
                "foo-bar.fint-core.entity.utdanning-vurdering-relation-update",
                any(),
                any(),
            )
        }
    }

    @Test
    fun `produce sends with correct key`() {
        val relationUpdate = createRelationUpdate("utdanning", "vurdering", "elevfravar", relationName = "elev")

        producer.produce(relationUpdate, "elev", "abc123")

        verify {
            kafkaTemplate.send(
                any(),
                "elev/abc123#elevfravar#elev",
                any(),
            )
        }
    }

    @Test
    fun `produce sends the relation update as value`() {
        val relationUpdate = createRelationUpdate("utdanning", "vurdering", "elevfravar")

        producer.produce(relationUpdate, "elev", "abc123")

        verify { kafkaTemplate.send(any(), any(), relationUpdate) }
    }

    private fun createRelationUpdate(
        domainName: String,
        packageName: String,
        resourceName: String,
        relationName: String = "relation",
    ) = RelationUpdate(
        targetEntity = createEntityDescriptor(domainName, packageName, resourceName),
        targetIds = listOf("id1"),
        binding =
            RelationBinding(
                relationName = relationName,
                link = Link.with("resource/id"),
            ),
        operation = RelationOperation.ADD,
    )
}
