package no.fintlabs.kafka.config

import no.fintlabs.kafka.KafkaConsumerNames
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaConsumerConfigTest {
    private fun consumerFactory(vararg extraProps: Pair<String, Any>): DefaultKafkaConsumerFactory<String, Any> =
        DefaultKafkaConsumerFactory(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ) + extraProps,
        )

    private fun kafkaProperties(
        resourceConcurrency: Int = 1,
        autoRelationConcurrency: Int = 1,
        relationUpdateConcurrency: Int = 1,
        eventConcurrency: Int = 1,
    ) = KafkaProperties(
        consumers =
            mapOf(
                KafkaConsumerNames.RESOURCE to KafkaProperties.ConsumerProperties(concurrency = resourceConcurrency),
                KafkaConsumerNames.AUTO_RELATION_RESOURCE to
                    KafkaProperties.ConsumerProperties(concurrency = autoRelationConcurrency),
                KafkaConsumerNames.RELATION_UPDATE to
                    KafkaProperties.ConsumerProperties(concurrency = relationUpdateConcurrency),
                KafkaConsumerNames.EVENT to KafkaProperties.ConsumerProperties(concurrency = eventConcurrency),
            ),
    )

    // region — shared ConsumerFactory

    @Test
    fun `all factories share the same ConsumerFactory instance`() {
        val shared = consumerFactory()
        val config = KafkaConsumerConfig(kafkaProperties(), shared)

        val factories =
            listOf(
                config.resourceFactory(),
                config.autoRelationResourceFactory(),
                config.relationUpdateFactory(),
                config.eventFactory(),
            )

        factories.forEach { factory ->
            assertTrue(
                factory.consumerFactory === shared,
                "${factory.factoryName()} should reference the shared ConsumerFactory, not a copy",
            )
        }
    }

    @Test
    fun `global fetch properties are visible on all factories`() {
        val shared =
            consumerFactory(
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG to 50_000,
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to 500,
            )
        val config = KafkaConsumerConfig(kafkaProperties(), shared)

        listOf(
            config.resourceFactory(),
            config.autoRelationResourceFactory(),
            config.relationUpdateFactory(),
            config.eventFactory(),
        ).forEach { factory ->
            val props = (factory.consumerFactory as DefaultKafkaConsumerFactory).configurationProperties
            assertEquals(
                50_000,
                props[ConsumerConfig.FETCH_MIN_BYTES_CONFIG],
                "${factory.factoryName()} is missing fetch.min.bytes — global consumer properties are not shared",
            )
            assertEquals(
                500,
                props[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG],
                "${factory.factoryName()} is missing fetch.max.wait.ms — global consumer properties are not shared",
            )
        }
    }

    // endregion

    // region — concurrency wiring

    @Test
    fun `each factory gets its own concurrency - not another consumer's`() {
        // Each consumer has a unique concurrency so cross-wiring is caught
        val config =
            KafkaConsumerConfig(
                kafkaProperties(
                    resourceConcurrency = 2,
                    autoRelationConcurrency = 3,
                    relationUpdateConcurrency = 4,
                    eventConcurrency = 5,
                ),
                consumerFactory(),
            )

        assertEquals(
            2,
            config.resourceFactory().concurrency,
            "resourceFactory has wrong concurrency — may be wired to wrong KafkaProperties entry",
        )
        assertEquals(
            3,
            config.autoRelationResourceFactory().concurrency,
            "autoRelationResourceFactory has wrong concurrency — may be wired to wrong KafkaProperties entry",
        )
        assertEquals(
            4,
            config.relationUpdateFactory().concurrency,
            "relationUpdateFactory has wrong concurrency — may be wired to wrong KafkaProperties entry",
        )
        assertEquals(
            5,
            config.eventFactory().concurrency,
            "eventFactory has wrong concurrency — may be wired to wrong KafkaProperties entry",
        )
    }

    @Test
    fun `concurrency defaults to 1 when consumer is missing from KafkaProperties`() {
        val config = KafkaConsumerConfig(KafkaProperties(consumers = emptyMap()), consumerFactory())

        assertEquals(1, config.resourceFactory().concurrency)
        assertEquals(1, config.autoRelationResourceFactory().concurrency)
        assertEquals(1, config.relationUpdateFactory().concurrency)
        assertEquals(1, config.eventFactory().concurrency)
    }

    // endregion

    private fun ConcurrentKafkaListenerContainerFactory<*, *>.factoryName(): String =
        this::class.simpleName ?: "UnknownFactory"

    private val ConcurrentKafkaListenerContainerFactory<*, *>.concurrency: Int
        get() = javaClass.getDeclaredField("concurrency").also { it.isAccessible = true }.get(this) as Int
}
