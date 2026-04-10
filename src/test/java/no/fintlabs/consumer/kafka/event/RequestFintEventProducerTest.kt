package no.fintlabs.consumer.kafka.event

import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.adapter.models.event.RequestFintEvent
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate

class RequestFintEventProducerTest {
    private val kafkaTemplate = mockk<KafkaTemplate<String, RequestFintEvent>>(relaxed = true)
    private val eventRequestTopicPattern = "foo-bar.fint-core.event.utdanning-vurdering-request"

    private lateinit var producer: RequestFintEventProducer

    @BeforeEach
    fun setUp() {
        producer = RequestFintEventProducer(kafkaTemplate, eventRequestTopicPattern)
    }

    @Test
    fun `produce sends with corrId as key`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.produce(event)

        verify { kafkaTemplate.send(any(), "abc-123", any()) }
    }

    @Test
    fun `produce sends to the configured topic`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.produce(event)

        verify { kafkaTemplate.send(eventRequestTopicPattern, any(), any()) }
    }

    @Test
    fun `produce sends the event as value`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.produce(event)

        verify { kafkaTemplate.send(any(), any(), event) }
    }
}
