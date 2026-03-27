package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.KafkaConfiguration
import no.fintlabs.consumer.config.OrgId
import no.fintlabs.consumer.resource.ResourceConverter
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ParameterizedListenerContainerFactory
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNameParameters
import no.novari.kafka.topic.name.TopicNameParameters
import no.novari.metamodel.MetamodelService
import no.novari.metamodel.model.Component
import no.novari.metamodel.model.Resource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import java.util.function.Consumer
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EntityConsumerTest {
    private lateinit var entityProcessingService: EntityProcessingService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var resourceConverter: ResourceConverter
    private lateinit var metamodelService: MetamodelService
    private lateinit var factoryService: ParameterizedListenerContainerFactoryService
    private lateinit var errorHandlerFactory: ErrorHandlerFactory
    private lateinit var factory: ParameterizedListenerContainerFactory<Any>
    private lateinit var container: ConcurrentMessageListenerContainer<String, Any>
    private lateinit var entityConsumer: EntityConsumer

    @BeforeEach
    fun setUp() {
        entityProcessingService = mockk(relaxed = true)
        consumerConfig = mockk()
        resourceConverter = mockk(relaxed = true)
        metamodelService = mockk()
        factoryService = mockk()
        errorHandlerFactory = mockk(relaxed = true)
        factory = mockk()
        container = mockk(relaxed = true)

        every { consumerConfig.orgId } returns OrgId.from("foo.bar")
        every { consumerConfig.domain } returns "utdanning"
        every { consumerConfig.packageName } returns "vurdering"

        every {
            factoryService.createRecordListenerContainerFactory(
                any<Class<Any>>(),
                any<Consumer<ConsumerRecord<String, Any>>>(),
                any(),
                any(),
            )
        } returns factory
        every { factory.createContainer(any<Collection<TopicNameParameters>>()) } returns container

        entityConsumer = EntityConsumer(entityProcessingService, consumerConfig, resourceConverter, metamodelService)
    }

    @Test
    fun `when consumeLegacyResourceTopics is disabled, only component topic is consumed`() {
        every { consumerConfig.kafka } returns KafkaConfiguration(consumeLegacyResourceTopics = false)

        val capturedTopics = slot<Collection<TopicNameParameters>>()
        every { factory.createContainer(capture(capturedTopics)) } returns container

        entityConsumer.resourceEntityConsumerFactory(factoryService, errorHandlerFactory)

        val topics = capturedTopics.captured.filterIsInstance<EntityTopicNameParameters>()
        assertEquals(1, topics.size)
        assertEquals("utdanning-vurdering", topics.first().resourceName)
    }

    @Test
    fun `when consumeLegacyResourceTopics is enabled, component topic and one topic per resource are consumed`() {
        every { consumerConfig.kafka } returns KafkaConfiguration(consumeLegacyResourceTopics = true)

        val component = mockk<Component>()
        val resource1 = mockk<Resource>()
        val resource2 = mockk<Resource>()
        every { resource1.name } returns "elevfravar"
        every { resource2.name } returns "eksamenskarakter"
        every { component.resources } returns listOf(resource1, resource2)
        every { metamodelService.getComponent("utdanning", "vurdering") } returns component

        val capturedTopics = slot<Collection<TopicNameParameters>>()
        every { factory.createContainer(capture(capturedTopics)) } returns container

        entityConsumer.resourceEntityConsumerFactory(factoryService, errorHandlerFactory)

        val topics = capturedTopics.captured.filterIsInstance<EntityTopicNameParameters>()
        assertEquals(3, topics.size)
        val resourceNames = topics.map { it.resourceName }
        assertTrue(resourceNames.contains("utdanning-vurdering"))
        assertTrue(resourceNames.contains("utdanning-vurdering-elevfravar"))
        assertTrue(resourceNames.contains("utdanning-vurdering-eksamenskarakter"))
    }
}
