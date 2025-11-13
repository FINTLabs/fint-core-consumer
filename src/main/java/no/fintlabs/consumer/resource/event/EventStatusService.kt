package no.fintlabs.consumer.resource.event

import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.models.event.ResponseFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URI

@Slf4j
@Service
@RequiredArgsConstructor
class EventStatusService(
    private val configuration: ConsumerConfiguration,
    private val eventService: EventService,
    private val linkService: LinkService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun getStatusResponse(
        resourceName: String,
        corrId: String,
    ): EventResponse =
        eventService
            .getResponse(corrId)
            ?.let { response ->
                takeIf { notError(response) }
                    ?.let { handleSuccessfulEvent(resourceName, corrId, response) }
                    ?: handleFailedEvents(resourceName, corrId, response)
            }?.also { logStatus(corrId, it) }
            ?: handleUnfinishedEvent(corrId).also { logStatus(corrId, it) }

    private fun handleUnfinishedEvent(corrId: String) =
        takeIf { eventService.requestExists(corrId) }
            ?.let { createAcceptedResponse() }
            ?: createNotFoundResponse()

    private fun handleFailedEvents(
        resourceName: String,
        corrId: String,
        responseFintEvent: ResponseFintEvent,
    ): EventResponse =
        if (responseFintEvent.isFailed) {
            createFailedResponse(responseFintEvent)
        } else if (responseFintEvent.isRejected) {
            createRejectedResponse(responseFintEvent)
        } else if (responseFintEvent.isConflicted) {
            mapCachedResourceAndSomethingElse(resourceName, corrId, responseFintEvent)
        } else {
            throw RuntimeException("Shouldn't happen")
        }

    private fun notError(response: ResponseFintEvent): Boolean = !(response.isFailed || response.isRejected || response.isConflicted)

    private fun handleSuccessfulEvent(
        resourceName: String,
        corrId: String,
        responseFintEvent: ResponseFintEvent,
    ): EventResponse =
        getEventResponseFromOperation(responseFintEvent)
            ?: mapCachedResourceAndSomethingElse(resourceName, corrId, responseFintEvent)

    private fun mapCachedResourceAndSomethingElse(
        resourceName: String,
        corrId: String,
        responseFintEvent: ResponseFintEvent,
    ): EventResponse =
        eventService.getResource(resourceName, corrId)?.let { resource ->
            linkService.mapLinks(resourceName, resource)
            if (responseFintEvent.isConflicted) {
                createConflictedResponse(resource)
            }
            createCreatedResponse(resource, createLocationUri(resourceName, resource))
        } ?: throw RuntimeException("No resource found for $corrId")

    private fun getEventResponseFromOperation(responseFintEvent: ResponseFintEvent): EventResponse? =
        when (responseFintEvent.operationType) {
            OperationType.VALIDATE -> createValidatedResponse(responseFintEvent)
            OperationType.DELETE -> createDeletedResponse()
            else -> null
        }

    fun createStatusHref(requestFintEvent: RequestFintEvent): String =
        "${configuration.componentUrl}/${requestFintEvent.resourceName}/status/${requestFintEvent.corrId}"

    fun createEntityHref(
        resourceName: String,
        idField: String,
        idValue: String,
    ): String = "${configuration.componentUrl}/$resourceName/${idField.lowercase()}/$idValue"

    private fun createLocationUri(
        resourceName: String,
        fintResource: FintResource,
    ): URI =
        fintResource.identifikators.entries
            .firstOrNull { it.value != null }
            ?.let { (idField, identifikator) ->
                URI.create(
                    createEntityHref(
                        resourceName,
                        idField,
                        identifikator.identifikatorverdi,
                    ),
                )
            }
            ?: throw RuntimeException("Resource has no identifier")

    private fun logStatus(
        corrId: String,
        eventResponse: EventResponse,
    ) = logger.info("Returned ${eventResponse.type.status} to user for $corrId")
}
