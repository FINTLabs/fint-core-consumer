package no.fintlabs.consumer.kafka.event

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.event.EventProducer
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.topic.EventTopicService
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RequestFintEventProducerTest {
    private val eventProducerFactory = mockk<EventProducerFactory>()
    private val eventTopicService = mockk<EventTopicService>(relaxed = true)
    private val config = mockk<ConsumerConfiguration>()
    private val kafkaProducer = mockk<EventProducer<RequestFintEvent>>(relaxed = true)

    private lateinit var producer: RequestFintEventProducer

    @BeforeEach
    fun setUp() {
        every { config.domain } returns "utdanning"
        every { config.packageName } returns "vurdering"
        every { eventProducerFactory.createProducer(RequestFintEvent::class.java) } returns kafkaProducer

        producer = RequestFintEventProducer(eventProducerFactory, eventTopicService, config)
    }

    @Test
    fun `publish sends event with corrId as key`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.publish("elevfravar", event)

        verify { kafkaProducer.send(match { it.key == "abc-123" }) }
    }

    @Test
    fun `publish builds topic name from domain, package and resource`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.publish("elevfravar", event)

        verify { kafkaProducer.send(match { it.topicNameParameters.eventName == "utdanning-vurdering-elevfravar" }) }
    }

    @Test
    fun `ensureTopic is only called once for the same resource`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.publish("elevfravar", event)
        producer.publish("elevfravar", event)

        verify(exactly = 1) { eventTopicService.ensureTopic(any(), any()) }
    }

    @Test
    fun `ensureTopic is called separately for different resources`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.publish("elevfravar", event)
        producer.publish("eksamensgruppe", event)

        verify(exactly = 2) { eventTopicService.ensureTopic(any(), any()) }
    }
}
