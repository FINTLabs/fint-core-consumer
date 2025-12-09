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

/**
 * Service responsible for checking the status of events and generating the appropriate [EventResponse].
 *
 * This service interacts with [EventService] to retrieve event data and [LinkService] to handle resource links.
 * It manages the flow of determining the correct response based on whether an event is finished, failed,
 * rejected, conflicted, or still in progress.
 */
@Slf4j
@Service
@RequiredArgsConstructor
class EventStatusService(
    private val configuration: ConsumerConfiguration,
    private val eventService: EventService,
    private val linkService: LinkService,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Retrieves the current processing status of an event identified by its correlation ID.
     *
     * If the event is finished, it returns the result (Created, Updated, Failed, Conflicted).
     * If the event is still processing, it returns an Accepted status.
     * If the correlation ID is unknown, it returns a NotFound status.
     *
     * @param resourceName The name of the resource (e.g., "student", "employee").
     * @param corrId The unique correlation ID associated with the original request.
     * @return An [EventResponse] indicating the state of the operation.
     */
    fun getStatusResponse(
        resourceName: String,
        corrId: String,
    ): EventResponse =
        eventService
            .getResponse(corrId)
            ?.let { handleResponse(resourceName, corrId, it) }
            ?.also { logStatus(corrId, it) }
            ?: handleUnfinishedEvent(corrId).also { logStatus(corrId, it) }

    private fun handleResponse(
        resourceName: String,
        corrId: String,
        response: ResponseFintEvent,
    ) = takeIf { notError(response) }
        ?.let { handleSuccessfulEvent(resourceName, corrId, response) }
        ?: handleFailedEvents(resourceName, corrId, response)

    private fun notError(response: ResponseFintEvent): Boolean = !(response.isFailed || response.isRejected || response.isConflicted)

    private fun handleSuccessfulEvent(
        resourceName: String,
        corrId: String,
        responseFintEvent: ResponseFintEvent,
    ): EventResponse =
        when (responseFintEvent.operationType) {
            OperationType.VALIDATE -> createValidatedResponse(responseFintEvent)
            OperationType.DELETE -> createDeletedResponse()
            OperationType.CREATE -> getResourceFromCache(resourceName, corrId, responseFintEvent)
            OperationType.UPDATE -> getResourceFromCache(resourceName, corrId, responseFintEvent)
        }

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
            getResourceFromCache(resourceName, corrId, responseFintEvent)
        } else {
            throw IllegalStateException(
                "Event response is considered an error, but no specific error flag (failed, rejected, conflicted) is set.",
            )
        }

    private fun handleUnfinishedEvent(corrId: String) =
        takeIf { eventService.requestExists(corrId) }
            ?.let { createAcceptedResponse() }
            ?: createNotFoundResponse()

    /**
     * Retrieves the actual resource payload from the cache and maps links.
     *
     * This is used when an operation (CREATE/UPDATE/CONFLICT) returns a resource body
     * that needs to be presented back to the client.
     */
    private fun getResourceFromCache(
        resourceName: String,
        corrId: String,
        responseFintEvent: ResponseFintEvent,
    ): EventResponse =
        eventService
            .getResource(resourceName, corrId)
            ?.let { resource ->
                linkService.mapLinks(resourceName, resource)
                when (responseFintEvent.isConflicted) {
                    true -> createConflictedResponse(resource)
                    false -> createCreatedResponse(resource, createLocationUri(resourceName, resource))
                }
            } ?: handleUnfinishedEvent(corrId)

    /**
     * Creates a URL string pointing to the status endpoint for a specific event.
     *
     * @param requestFintEvent The request event containing the resource name and correlation ID.
     * @return A string representing the status URL.
     */
    fun createStatusHref(requestFintEvent: RequestFintEvent): String =
        "${configuration.componentUrl}/${requestFintEvent.resourceName}/status/${requestFintEvent.corrId}"

    /**
     * Creates a URL string pointing to a specific entity resource.
     *
     * @param resourceName The name of the resource.
     * @param idField The name of the identifier field.
     * @param idValue The value of the identifier.
     * @return A string representing the entity URL.
     */
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
