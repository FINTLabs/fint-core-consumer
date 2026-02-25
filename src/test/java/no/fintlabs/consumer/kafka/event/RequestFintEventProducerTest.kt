package no.fintlabs.consumer.kafka.event

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.name.EventTopicNameParameters
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RequestFintEventProducerTest {
    private val parameterizedTemplateFactory = mockk<ParameterizedTemplateFactory>()
    private val eventTopicService = mockk<EventTopicService>(relaxed = true)
    private val config = mockk<ConsumerConfiguration>()
    private val kafkaTemplate = mockk<ParameterizedTemplate<RequestFintEvent>>(relaxed = true)

    private lateinit var producer: RequestFintEventProducer

    @BeforeEach
    fun setUp() {
        every { config.domain } returns "utdanning"
        every { config.packageName } returns "vurdering"
        every { config.orgId } returns "fintlabs.no"
        every { parameterizedTemplateFactory.createTemplate(RequestFintEvent::class.java) } returns kafkaTemplate

        producer = RequestFintEventProducer(parameterizedTemplateFactory, eventTopicService, config)
    }

    @Test
    fun `publish sends event with corrId as key`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.publish("elevfravar", event)

        verify { kafkaTemplate.send(match<ParameterizedProducerRecord<RequestFintEvent>> { it.key == "abc-123" }) }
    }

    @Test
    fun `publish builds event name from domain, package and resource`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.publish("elevfravar", event)

        verify {
            kafkaTemplate.send(
                match<ParameterizedProducerRecord<RequestFintEvent>> {
                    (it.topicNameParameters as EventTopicNameParameters).eventName == "utdanning-vurdering-elevfravar-request"
                },
            )
        }
    }

    @Test
    fun `ensureTopic is only called once for the same resource`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.publish("elevfravar", event)
        producer.publish("elevfravar", event)

        verify(exactly = 1) { eventTopicService.createOrModifyTopic(any(), any()) }
    }

    @Test
    fun `ensureTopic is called separately for different resources`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.publish("elevfravar", event)
        producer.publish("eksamensgruppe", event)

        verify(exactly = 2) { eventTopicService.createOrModifyTopic(any(), any()) }
    }
}
