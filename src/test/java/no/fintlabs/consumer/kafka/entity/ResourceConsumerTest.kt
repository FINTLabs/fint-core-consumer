package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.KafkaConfiguration
import no.fintlabs.consumer.config.OrgId
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.resource.ResourceConverter
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ParameterizedListenerContainerFactory
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNamePatternParameters
import no.novari.kafka.topic.name.TopicNamePatternParameters
import no.novari.metamodel.MetamodelService
import no.novari.metamodel.model.Component
import no.novari.metamodel.model.Resource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import java.util.Optional
import java.util.function.Consumer
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ResourceConsumerTest {
    private lateinit var resourceProcessingService: ResourceProcessingService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var resourceConverter: ResourceConverter
    private lateinit var metamodelService: MetamodelService
    private lateinit var factoryService: ParameterizedListenerContainerFactoryService
    private lateinit var errorHandlerFactory: ErrorHandlerFactory
    private lateinit var factory: ParameterizedListenerContainerFactory<Any>
    private lateinit var container: ConcurrentMessageListenerContainer<String, Any>
    private lateinit var resourceConsumer: ResourceConsumer

    @BeforeEach
    fun setUp() {
        resourceProcessingService = mockk(relaxed = true)
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
        every { factory.createContainer(any<TopicNamePatternParameters>()) } returns container

        resourceConsumer =
            ResourceConsumer(resourceProcessingService, consumerConfig, resourceConverter, metamodelService)
    }

    @Test
    fun `when consumeLegacyResourceTopics is disabled, only component topic is consumed`() {
        every { consumerConfig.kafka } returns KafkaConfiguration(consumeLegacyResourceTopics = false)

        val captured = slot<EntityTopicNamePatternParameters>()
        every { factory.createContainer(capture(captured)) } returns container

        resourceConsumer.resourceEntityConsumerFactory(factoryService, errorHandlerFactory)

        val resourcePattern =
            captured.captured.topicNamePatternSuffixParameters
                .first()
                .pattern
        assertEquals(listOf("utdanning-vurdering"), resourcePattern.anyOfValues)
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

        val captured = slot<EntityTopicNamePatternParameters>()
        every { factory.createContainer(capture(captured)) } returns container

        resourceConsumer.resourceEntityConsumerFactory(factoryService, errorHandlerFactory)

        val resourcePattern =
            captured.captured.topicNamePatternSuffixParameters
                .first()
                .pattern
        assertEquals(3, resourcePattern.anyOfValues.size)
        assertTrue(resourcePattern.anyOfValues.contains("utdanning-vurdering"))
        assertTrue(resourcePattern.anyOfValues.contains("utdanning-vurdering-elevfravar"))
        assertTrue(resourcePattern.anyOfValues.contains("utdanning-vurdering-eksamenskarakter"))
    }

    @Test
    fun `when consumeLegacyResourceTopics is disabled and header is present, resource name is read from header`() {
        every { consumerConfig.kafka } returns KafkaConfiguration(consumeLegacyResourceTopics = false)

        val captured = slot<ResourceConsumerRecord>()
        every { resourceProcessingService.processResourceConsumerRecord(capture(captured)) } returns Unit

        resourceConsumer.consumeRecord(
            createConsumerRecord(
                topic = "utdanning-vurdering",
                resourceNameHeader = "elevfravar",
            ),
        )

        assertEquals("elevfravar", captured.captured.resourceName)
    }

    @Test
    fun `when consumeLegacyResourceTopics is disabled and header is missing, an exception is thrown`() {
        every { consumerConfig.kafka } returns KafkaConfiguration(consumeLegacyResourceTopics = false)

        assertThrows<IllegalArgumentException> {
            resourceConsumer.consumeRecord(
                createConsumerRecord(topic = "utdanning-vurdering", resourceNameHeader = null),
            )
        }
    }

    @Test
    fun `when consumeLegacyResourceTopics is enabled and header is present, resource name is read from header`() {
        every { consumerConfig.kafka } returns KafkaConfiguration(consumeLegacyResourceTopics = true)

        val captured = slot<ResourceConsumerRecord>()
        every { resourceProcessingService.processResourceConsumerRecord(capture(captured)) } returns Unit

        resourceConsumer.consumeRecord(
            createConsumerRecord(
                topic = "utdanning-vurdering-elevfravar",
                resourceNameHeader = "elevfravar",
            ),
        )

        assertEquals("elevfravar", captured.captured.resourceName)
    }

    @Suppress("ktlint:standard:max-line-length")
    @Test
    fun `when consumeLegacyResourceTopics is enabled and header is missing, resource name falls back to last topic segment`() {
        every { consumerConfig.kafka } returns KafkaConfiguration(consumeLegacyResourceTopics = true)

        val captured = slot<ResourceConsumerRecord>()
        every { resourceProcessingService.processResourceConsumerRecord(capture(captured)) } returns Unit

        resourceConsumer.consumeRecord(
            createConsumerRecord(
                topic = "utdanning-vurdering-elevfravar",
                resourceNameHeader = null,
            ),
        )

        assertEquals("elevfravar", captured.captured.resourceName)
    }

    private fun createConsumerRecord(
        topic: String,
        resourceNameHeader: String?,
    ): ConsumerRecord<String, Any?> {
        val headers = RecordHeaders()
        headers.add(
            RecordHeader(
                LAST_MODIFIED,
                java.nio.ByteBuffer
                    .allocate(Long.SIZE_BYTES)
                    .putLong(0L)
                    .array(),
            ),
        )
        if (resourceNameHeader != null) {
            headers.add(RecordHeader(RESOURCE_NAME, resourceNameHeader.toByteArray()))
        }
        return ConsumerRecord(
            topic,
            0,
            0,
            0L,
            TimestampType.CREATE_TIME,
            NULL_SIZE,
            NULL_SIZE,
            "key",
            null,
            headers,
            Optional.empty<Int>(),
        )
    }
}
