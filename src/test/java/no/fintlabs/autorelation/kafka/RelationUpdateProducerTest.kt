package no.fintlabs.autorelation.kafka

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.KafkaConfiguration
import no.fintlabs.consumer.config.OrgId
import no.fintlabs.consumer.kafka.KafkaThroughputMetrics
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EntityTopicService
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.metamodel.MetamodelService
import no.novari.metamodel.model.Component
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RelationUpdateProducerTest {
    private lateinit var entityTopicService: EntityTopicService
    private lateinit var parameterizedTemplateFactory: ParameterizedTemplateFactory
    private lateinit var consumerConfiguration: ConsumerConfiguration
    private lateinit var kafkaThroughputMetrics: KafkaThroughputMetrics
    private lateinit var metamodelService: MetamodelService
    private lateinit var template: ParameterizedTemplate<RelationUpdate>

    @BeforeEach
    fun setUp() {
        entityTopicService = mockk(relaxed = true)
        parameterizedTemplateFactory = mockk()
        consumerConfiguration = mockk()
        kafkaThroughputMetrics = mockk(relaxed = true)
        metamodelService = mockk()
        template = mockk()

        every { parameterizedTemplateFactory.createTemplate(RelationUpdate::class.java) } returns template
        every { consumerConfiguration.orgId } returns OrgId.from("foo.bar")
        every { consumerConfiguration.kafka } returns KafkaConfiguration()

        val component1 =
            mockk<Component>().also {
                every { it.domainName } returns "utdanning"
                every { it.packageName } returns "vurdering"
            }
        val component2 =
            mockk<Component>().also {
                every { it.domainName } returns "utdanning"
                every { it.packageName } returns "elev"
            }
        every { metamodelService.getComponents() } returns listOf(component1, component2)
    }

    @Test
    fun `when ensureTopics is true, createOrModifyTopic is called for each component`() {
        every { consumerConfiguration.kafka } returns KafkaConfiguration(ensureTopics = true)

        RelationUpdateProducer(
            entityTopicService,
            parameterizedTemplateFactory,
            consumerConfiguration,
            kafkaThroughputMetrics,
            metamodelService,
        )

        verify(exactly = 2) { entityTopicService.createOrModifyTopic(any<EntityTopicNameParameters>(), any()) }
    }

    @Test
    fun `when ensureTopics is false, createOrModifyTopic is never called`() {
        every { consumerConfiguration.kafka } returns KafkaConfiguration(ensureTopics = false)

        RelationUpdateProducer(
            entityTopicService,
            parameterizedTemplateFactory,
            consumerConfiguration,
            kafkaThroughputMetrics,
            metamodelService,
        )

        verify(exactly = 0) { entityTopicService.createOrModifyTopic(any<EntityTopicNameParameters>(), any()) }
    }

    @Test
    fun `when ensureTopics is true by default, createOrModifyTopic is called for each component`() {
        every { consumerConfiguration.kafka } returns KafkaConfiguration()

        RelationUpdateProducer(
            entityTopicService,
            parameterizedTemplateFactory,
            consumerConfiguration,
            kafkaThroughputMetrics,
            metamodelService,
        )

        verify(exactly = 2) { entityTopicService.createOrModifyTopic(any<EntityTopicNameParameters>(), any()) }
    }
}
