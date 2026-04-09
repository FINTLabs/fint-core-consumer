package no.fintlabs.consumer.kafka.event

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.OrgId
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
        every { config.orgId } returns OrgId.from("fintlabs.no")
        every { config.kafka } returns
            mockk {
                every { requestPartitions } returns 1
                every { requestRetentionTime } returns java.time.Duration.ofDays(7)
            }
        every { parameterizedTemplateFactory.createTemplate(RequestFintEvent::class.java) } returns kafkaTemplate

        producer = RequestFintEventProducer(parameterizedTemplateFactory, eventTopicService, config)
    }

    @Test
    fun `publish sends event with corrId as key`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.produce(event)

        verify { kafkaTemplate.send(match<ParameterizedProducerRecord<RequestFintEvent>> { it.key == "abc-123" }) }
    }

    @Test
    fun `publish builds event name from domain and package`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.produce(event)

        verify {
            kafkaTemplate.send(
                match<ParameterizedProducerRecord<RequestFintEvent>> {
                    (it.topicNameParameters as EventTopicNameParameters).eventName == "utdanning-vurdering-request"
                },
            )
        }
    }

    @Test
    fun `ensureTopic is called once during construction`() {
        verify(exactly = 1) { eventTopicService.createOrModifyTopic(any(), any()) }
    }

    @Test
    fun `multiple publishes do not create additional topics`() {
        val event = RequestFintEvent().apply { corrId = "abc-123" }

        producer.produce(event)
        producer.produce(event)

        verify(exactly = 1) { eventTopicService.createOrModifyTopic(any(), any()) }
    }
}
