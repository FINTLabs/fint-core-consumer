package no.fintlabs.consumer.resource

import io.mockk.every
import io.mockk.mockk
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.event.EventBodyResponse
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RequestFintEventProducer
import no.fintlabs.consumer.resource.event.*
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
        fun `should return 204 NO_CONTENT when request status is ResourceDeleted`() {
            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns ResourceDeleted

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.NO_CONTENT, responseEntity.statusCode)
            assertNull(responseEntity.body)
            assertNull(responseEntity.headers.location)
        }

        @Test
        fun `should return 202 ACCEPTED when request status is RequestAccepted`() {
            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns RequestAccepted

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.ACCEPTED, responseEntity.statusCode)
            assertNull(responseEntity.body)
            assertNull(responseEntity.headers.location)
        }

        @Test
        fun `should return 410 GONE when request status is RequestGone`() {
            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns RequestGone

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.GONE, responseEntity.statusCode)
            assertNull(responseEntity.body)
            assertNull(responseEntity.headers.location)
        }

        @Test
        fun `should return 502 INTERNAL_SERVER_ERROR and body when request status is RequestFailed with Error FailureType`() {
            val body = "123"

            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns
                RequestFailed(
                    body = body,
                    failureType = RequestFailed.FailureType.ERROR,
                )

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, responseEntity.statusCode)
            assertEquals(body, responseEntity.body)
            assertNull(responseEntity.headers.location)
        }

        @Test
        fun `should return 409 CONFLICT and body when request status is RequestFailed with Conflict FailureType`() {
            val body = "321"

            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns
                RequestFailed(
                    body = body,
                    failureType = RequestFailed.FailureType.CONFLICT,
                )

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.CONFLICT, responseEntity.statusCode)
            assertEquals(body, responseEntity.body)
            assertNull(responseEntity.headers.location)
        }

        @Test
        fun `should return 400 BAD_REQUEST and body when request status is RequestFailed with Rejected FailureType`() {
            val body = "213"

            every { requestStatusService.getStatusResponse(resourceName, corrId) } returns
                RequestFailed(
                    body = body,
                    failureType = RequestFailed.FailureType.REJECTED,
                )

            val responseEntity = sut.getStatus(resourceName, corrId)

            assertEquals(HttpStatus.BAD_REQUEST, responseEntity.statusCode)
            assertEquals(body, responseEntity.body)
            assertNull(responseEntity.headers.location)
        }
    }
}
