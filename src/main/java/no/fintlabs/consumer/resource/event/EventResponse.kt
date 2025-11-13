package no.fintlabs.consumer.resource.event

import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.event.EventBodyResponse
import no.fintlabs.adapter.models.event.ResponseFintEvent
import org.springframework.http.HttpStatus
import java.net.URI

data class EventResponse(
    val type: EventResponseType,
    val body: Any? = null,
    val location: URI? = null,
)

enum class EventResponseType(
    val status: HttpStatus,
) {
    ACCEPTED(HttpStatus.ACCEPTED),
    NOT_FOUND(HttpStatus.NOT_FOUND),
    VALIDATED(HttpStatus.OK),
    DELETED(HttpStatus.NO_CONTENT),
    FAILED(HttpStatus.INTERNAL_SERVER_ERROR),
    REJECTED(HttpStatus.BAD_REQUEST),
    CONFLICT(HttpStatus.CONFLICT),
    CREATED(HttpStatus.CREATED),
}

fun createValidatedResponse(responseFintEvent: ResponseFintEvent) =
    EventResponse(EventResponseType.VALIDATED, EventBodyResponse.ofResponseEvent(responseFintEvent))

fun createDeletedResponse() = EventResponse(EventResponseType.DELETED)

fun createFailedResponse(responseFintEvent: ResponseFintEvent) =
    EventResponse(EventResponseType.FAILED, EventBodyResponse.ofResponseEvent(responseFintEvent))

fun createRejectedResponse(responseFintEvent: ResponseFintEvent) =
    EventResponse(EventResponseType.REJECTED, EventBodyResponse.ofResponseEvent(responseFintEvent))

fun createConflictedResponse(resource: FintResource) = EventResponse(EventResponseType.CONFLICT, resource)

fun createCreatedResponse(
    resource: FintResource,
    location: URI,
) = EventResponse(EventResponseType.CREATED, resource, location)

fun createAcceptedResponse() = EventResponse(EventResponseType.ACCEPTED)

fun createNotFoundResponse() = EventResponse(EventResponseType.NOT_FOUND)
