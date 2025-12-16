package no.fintlabs.consumer.resource

import io.mockk.every
import io.mockk.mockk
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.event.EventBodyResponse
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RequestFintEventProducer
import no.fintlabs.consumer.resource.event.RequestGone
import no.fintlabs.consumer.resource.event.RequestStatusService
import no.fintlabs.consumer.resource.event.RequestValidated
import no.fintlabs.consumer.resource.event.ResourceCreated
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import org.springframework.http.HttpStatus
import java.net.URI
import java.util.*

class ResourceControllerTest {
    private val resourceService: ResourceService = mockk()
    private val requestFintEventProducer: RequestFintEventProducer = mockk()
    private val requestStatusService: RequestStatusService = mockk()
    private val consumerConfig: ConsumerConfiguration = mockk()
    private lateinit var sut: ResourceController

    private val resourceName = "elevfravar"
    private val corrId = UUID.randomUUID().toString()

    @BeforeEach
    fun setUp() {
        sut = ResourceController(resourceService, requestFintEventProducer, requestStatusService, consumerConfig)
    }

    @Nested
    inner class GetStatus {
        @Test
        fun `should return 201 CREATED, location header and body when request status is ResourceCreated`() {
            val resource =
                ElevfravarResource().apply {
                    systemId =
                        Identifikator().apply {
                            identifikatorverdi = corrId
                        }
                }
            val uri = URI.create("https://test.com/corrid/123")

            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns
                ResourceCreated(
                    body = resource,
                    location = uri,
                )

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.CREATED, responseEntity.statusCode)
            assertEquals(resource, responseEntity.body)
            assertEquals(uri, responseEntity.headers.location)
        }

        @Test
        fun `should return 200 OK and body when request status is RequestValidated`() {
            val eventBodyResponse =
                EventBodyResponse
                    .builder()
                    .message("test")
                    .build()

            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns
                RequestValidated(
                    eventBodyResponse,
                )

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.OK, responseEntity.statusCode)
            assertEquals(eventBodyResponse, responseEntity.body)
            assertNull(responseEntity.headers.location)
        }

        @Test
        fun `should return 410 GONE when operation status is GONE`() {
            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns RequestGone

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.GONE, responseEntity.statusCode)
            assertNull(responseEntity.body)
            assertNull(responseEntity.headers.location)
        }
    }
}
