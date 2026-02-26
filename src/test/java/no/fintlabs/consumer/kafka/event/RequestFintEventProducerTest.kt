package no.fintlabs.consumer.kafka.event

import com.fasterxml.jackson.databind.ObjectMapper
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.EventCacheProperties
import no.fintlabs.consumer.resource.ResourceService
import no.novari.fint.model.FintIdentifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.kafka.producing.ParameterizedTemplate
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

// A real class to hold data so Jackson has something actual to serialize
data class DummyFintResource(
    val name: String?,
    val id: Int?,
) : FintResource {
    override fun getLinks(): Map<String?, List<Link?>?> = emptyMap()

    override fun getIdentifikators(): Map<String?, FintIdentifikator?> = emptyMap()
}

@ExtendWith(MockKExtension::class)
class RequestFintEventProducerTest {
    @MockK
    private lateinit var parameterizedTemplateFactory: ParameterizedTemplateFactory

    @MockK(relaxed = true)
    private lateinit var eventTopicService: EventTopicService

    @MockK
    private lateinit var config: ConsumerConfiguration

    @MockK
    private lateinit var resourceService: ResourceService

    @MockK
    private lateinit var props: EventCacheProperties

    @MockK(relaxed = true)
    private lateinit var mockKafkaTemplate: ParameterizedTemplate<RequestFintEvent>

    private lateinit var requestFintEventProducer: RequestFintEventProducer

    private val realObjectMapper = ObjectMapper()

    private val fixedClock = Clock.fixed(Instant.parse("2024-01-01T12:00:00Z"), ZoneId.of("UTC"))

    @BeforeEach
    fun setUp() {
        every { parameterizedTemplateFactory.createTemplate(RequestFintEvent::class.java) } returns mockKafkaTemplate

        requestFintEventProducer =
            RequestFintEventProducer(
                parameterizedTemplateFactory,
                eventTopicService,
                config,
                resourceService,
                realObjectMapper,
                props,
                fixedClock,
            )

        val mockLifeCycleConfig = io.mockk.mockk<EventCacheProperties.LifeCycle>()
        every { config.orgId } returns "vigo.no"
        every { config.domain } returns "utdanning"
        every { config.packageName } returns "vurdering"
        every { mockLifeCycleConfig.ttl } returns Duration.ofHours(1)
        every { props.getLifeCycleConfig(any()) } returns mockLifeCycleConfig
    }

    @Test
    fun `sendEvent should actually serialize a populated object`() {
        val resourceData = mapOf("name" to "Test Group", "id" to 123)
        val realMappedResource = DummyFintResource("Test Group", 123)

        every { resourceService.mapResourceAndLinks("eksamensgruppe", resourceData) } returns realMappedResource

        val resultEvent = requestFintEventProducer.sendEvent("eksamensgruppe", resourceData, OperationType.CREATE)

        assertEquals("""{"name":"Test Group","id":123,"identifikators":{},"_links":null}""", resultEvent.value)
    }

    @Test
    fun `sendEvent should actually serialize an empty object`() {
        val emptyData = emptyMap<String, Any>()
        val emptyMappedResource = DummyFintResource(null, null)

        every { resourceService.mapResourceAndLinks("eksamensgruppe", emptyData) } returns emptyMappedResource

        val resultEvent = requestFintEventProducer.sendEvent("eksamensgruppe", emptyData, OperationType.CREATE)

        assertEquals("""{"name":null,"id":null,"identifikators":{},"_links":null}""", resultEvent.value)
    }

    @Test
    fun `sendEvent should actually serialize when resourceData is null`() {
        val nullData: Any? = null

        val emptyMappedResource = DummyFintResource(null, null)

        every { resourceService.mapResourceAndLinks("eksamensgruppe", nullData) } returns emptyMappedResource

        val resultEvent = requestFintEventProducer.sendEvent("eksamensgruppe", nullData, OperationType.CREATE)

        assertEquals("""{"name":null,"id":null,"identifikators":{},"_links":null}""", resultEvent.value)
    }

}
