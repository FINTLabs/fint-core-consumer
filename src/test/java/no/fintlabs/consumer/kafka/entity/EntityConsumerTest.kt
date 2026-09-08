package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.KafkaConfiguration
import no.fintlabs.consumer.config.OrgId
import no.fintlabs.consumer.health.InitialKafkaBootstrapTracker
import no.fintlabs.consumer.health.KafkaListenerContainerHealthConfigurer
import no.fintlabs.consumer.health.KafkaRuntimeHealthMonitor
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.resource.ResourceConverter
import no.novari.kafka.consuming.ErrorHandlerFactory
import no.novari.kafka.consuming.ListenerConfiguration
import no.novari.kafka.consuming.ParameterizedListenerContainerFactory
import no.novari.kafka.consuming.ParameterizedListenerContainerFactoryService
import no.novari.kafka.topic.name.EntityTopicNamePatternParameters
import no.novari.kafka.topic.name.TopicNamePatternParameters
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.listener.ContainerProperties
import java.util.Optional
import java.util.function.Consumer
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EntityConsumerTest {
    private lateinit var entityProcessingService: EntityProcessingService
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var resourceConverter: ResourceConverter
    private lateinit var factoryService: ParameterizedListenerContainerFactoryService
    private lateinit var errorHandlerFactory: ErrorHandlerFactory
    private lateinit var factory: ParameterizedListenerContainerFactory<Any>
    private lateinit var container: ConcurrentMessageListenerContainer<String, Any>
    private lateinit var initialKafkaBootstrapTracker: InitialKafkaBootstrapTracker
    private lateinit var kafkaRuntimeHealthMonitor: KafkaRuntimeHealthMonitor
    private lateinit var kafkaListenerContainerHealthConfigurer: KafkaListenerContainerHealthConfigurer
    private lateinit var entityConsumer: EntityConsumer

    @BeforeEach
    fun setUp() {
        entityProcessingService = mockk(relaxed = true)
        consumerConfig = mockk()
        resourceConverter = mockk(relaxed = true)
        factoryService = mockk()
        errorHandlerFactory = mockk(relaxed = true)
        factory = mockk()
        initialKafkaBootstrapTracker = mockk(relaxed = true)
        kafkaRuntimeHealthMonitor = mockk(relaxed = true)
        kafkaListenerContainerHealthConfigurer = mockk(relaxed = true)
        container =
            ConcurrentMessageListenerContainer<String, Any>(
                mockk<ConsumerFactory<String, Any>>(relaxed = true),
                ContainerProperties("test-topic"),
            )

        every { consumerConfig.orgId } returns OrgId.from("foo.bar")
        every { consumerConfig.domain } returns "utdanning"
        every { consumerConfig.packageName } returns "vurdering"

        every {
            factoryService.createRecordListenerContainerFactory(
                any<Class<Any>>(),
                any<Consumer<ConsumerRecord<String, Any>>>(),
                any(),
                any(),
                any(),
            )
        } returns factory
        every { factory.createContainer(any<TopicNamePatternParameters>()) } returns container

        entityConsumer =
            EntityConsumer(
                entityProcessingService,
                consumerConfig,
                resourceConverter,
                initialKafkaBootstrapTracker,
                kafkaRuntimeHealthMonitor,
                kafkaListenerContainerHealthConfigurer,
            )
    }

    @Test
    fun `only component topic is consumed`() {
        every { consumerConfig.kafka } returns KafkaConfiguration()

        val captured = slot<EntityTopicNamePatternParameters>()
        every { factory.createContainer(capture(captured)) } returns container

        entityConsumer.resourceEntityConsumerFactory(factoryService, errorHandlerFactory)

        val resourcePattern =
            captured.captured.topicNamePatternSuffixParameters
                .first()
                .pattern
        assertEquals(listOf("utdanning-vurdering"), resourcePattern.anyOfValues)
    }

    @Test
    fun `container gets fetch and idle settings from consumer configuration`() {
        every {
            consumerConfig.kafka
        } returns KafkaConfiguration(fetchMinBytes = 12345, fetchMaxWaitMs = 678, idleBetweenPolls = 222)

        val customizer = slot<Consumer<ConcurrentMessageListenerContainer<String, Any>>>()
        every {
            factoryService.createRecordListenerContainerFactory(
                any<Class<Any>>(),
                any<Consumer<ConsumerRecord<String, Any>>>(),
                any(),
                any(),
                capture(customizer),
            )
        } answers {
            customizer.captured.accept(container)
            factory
        }

        val createdContainer = entityConsumer.resourceEntityConsumerFactory(factoryService, errorHandlerFactory)

        assertEquals(222L, createdContainer.containerProperties.idleBetweenPolls)
        assertEquals(
            "12345",
            createdContainer.containerProperties.kafkaConsumerProperties.getProperty(
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
            ),
        )
        assertEquals(
            "678",
            createdContainer.containerProperties.kafkaConsumerProperties.getProperty(
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
            ),
        )
    }

    @Test
    fun `when header is present, resource name is read from header`() {
        every { consumerConfig.kafka } returns KafkaConfiguration()

        val captured = slot<EntityConsumerRecord>()
        every { entityProcessingService.processEntityConsumerRecord(capture(captured)) } returns Unit

        entityConsumer.consumeRecord(
            createConsumerRecord(
                topic = "utdanning-vurdering",
                resourceNameHeader = "elevfravar",
            ),
        )

        assertEquals("elevfravar", captured.captured.resourceName)
    }

    @Test
    fun `when header is missing, an exception is thrown`() {
        every { consumerConfig.kafka } returns KafkaConfiguration()

        assertThrows<IllegalArgumentException> {
            entityConsumer.consumeRecord(createConsumerRecord(topic = "utdanning-vurdering", resourceNameHeader = null))
        }
    }

    @Test
    fun `listener configuration seeks to beginning on partition assignment`() {
        val config = captureListenerConfig()
        val callback = mockk<ConsumerSeekAware.ConsumerSeekCallback>(relaxed = true)
        val partition = TopicPartition("test-topic", 0)

        assertTrue(config.onPartitionsAssigned.isPresent)
        config.onPartitionsAssigned.get().accept(mapOf(partition to 0L), callback)

        verify { callback.seekToBeginning(setOf(partition)) }
    }

    private fun captureListenerConfig(): ListenerConfiguration {
        every { consumerConfig.kafka } returns KafkaConfiguration()
        val slot = slot<ListenerConfiguration>()
        every {
            factoryService.createRecordListenerContainerFactory(
                any<Class<Any>>(),
                any<Consumer<ConsumerRecord<String, Any>>>(),
                capture(slot),
                any(),
                any(),
            )
        } returns factory
        entityConsumer.resourceEntityConsumerFactory(factoryService, errorHandlerFactory)
        return slot.captured
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
