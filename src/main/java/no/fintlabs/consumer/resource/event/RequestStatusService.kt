package no.fintlabs.consumer.resource.event

import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.event.EventBodyResponse
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.models.event.ResponseFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceConverter
import org.springframework.stereotype.Service
import java.net.URI

/**
 * Service responsible for checking the status of events and generating the appropriate [OperationStatus].
 *
 * This service interacts with [EventService] to retrieve event data.
 * It manages the flow of determining the correct response based on whether an event is finished, failed,
 * rejected, conflicted, or still in progress.
 */
@Slf4j
@Service
@RequiredArgsConstructor
class RequestStatusService(
    private val configuration: ConsumerConfiguration,
    private val eventService: EventService,
    private val cacheService: CacheService,
    private val resourceConverter: ResourceConverter,
) {
    /**
     * The main entry point for status checks.
     * Determines if a request is done, in progress, or missing.
     *
     * @param resourceName The entity name (e.g., "student").
     * @param corrId The unique ID of the request.
     */
    fun getStatusResponse(
        resourceName: String,
        corrId: String,
    ): OperationStatus =
        eventService
            .getResponse(corrId)
            ?.let { handleFinishedEvent(resourceName, it) }
            ?: handleUnknownOrRunningEvent(corrId)

    /**
     * Creates a URL string pointing to the status endpoint for a specific event.
     *
     * @param requestFintEvent The request event containing the resource name and correlation ID.
     * @return A string representing the status URL.
     */
    fun createStatusLocation(requestFintEvent: RequestFintEvent): String =
        "${configuration.componentUrl}/${requestFintEvent.resourceName}/status/${requestFintEvent.corrId}"

    private fun handleFinishedEvent(
        resourceName: String,
        response: ResponseFintEvent,
    ) = if (response.isError()) {
        handleErrorResponse(resourceName, response)
    } else {
        handleSuccessfulResponse(resourceName, response)
    }

    private fun handleSuccessfulResponse(
        resourceName: String,
        response: ResponseFintEvent,
    ): OperationStatus =
        when (response.operationType) {
            OperationType.VALIDATE -> response.toStatus(OperationState.VALIDATED)

            OperationType.DELETE -> OperationStatus(OperationState.DELETED)

            // For Create/Update, we must verify the Cache contains the new data before confirming 201 Created
            OperationType.CREATE,
            OperationType.UPDATE,
            -> verifyResourceInCache(resourceName, response)
        }

    fun handleErrorResponse(
        resourceName: String,
        responseFintEvent: ResponseFintEvent,
    ): OperationStatus =
        if (responseFintEvent.isFailed) {
            responseFintEvent.toStatus(OperationState.FAILED)
        } else if (responseFintEvent.isRejected) {
            responseFintEvent.toStatus(OperationState.REJECTED)
        } else if (responseFintEvent.isConflicted) {
            OperationStatus(OperationState.CONFLICT, responseFintEvent.convertResource(resourceName))
        } else {
            throw IllegalStateException(
                "Event response is considered an error, but no specific error flag (failed, rejected, conflicted) is set.",
            )
        }

    private fun handleUnknownOrRunningEvent(corrId: String) =
        if (eventService.requestExists(corrId)) {
            OperationStatus(OperationState.ACCEPTED)
        } else {
            OperationStatus(OperationState.GONE)
        }

    /**
     * Checks if the resource resulting from the event has actually propagated to the Cache.
     *
     * IMPORTANT: Even if the Event is marked "Create", we cannot return [OperationState.CREATED]
     * until the CacheService has the updated object. If the cache is lagging,
     * we technically return [OperationState.ACCEPTED] to tell the client to wait a bit longer.
     */
    private fun verifyResourceInCache(
        resourceName: String,
        responseFintEvent: ResponseFintEvent,
    ): OperationStatus =
        responseFintEvent
            .getSyncedResource(resourceName)
            ?.toStatus()
            ?: handleUnknownOrRunningEvent(responseFintEvent.corrId)

    /**
     * Retrieves the resource from the cache ONLY IF the cache has "caught up"
     * with this specific event.
     *
     * Logic:
     * - [ResponseFintEvent.handledAt]: The timestamp when the Adapter finished processing.
     * - [no.fintlabs.cache.Cache.getLastDelivered]: The timestamp of the data currently sitting in the cache.
     *
     * If they match, the data in the cache is guaranteed to be the result of this event.
     */
    private fun ResponseFintEvent.getSyncedResource(resourceName: String): FintResource? =
        takeIf { resourceIsInCache(resourceName) }
            ?.let { getResourceFromIdentifier(resourceName) }

    private fun ResponseFintEvent.resourceIsInCache(resourceName: String): Boolean =
        cacheService.getCache(resourceName).getLastDelivered(value.identifier) == handledAt

    private fun ResponseFintEvent.getResourceFromIdentifier(resourceName: String): FintResource? =
        cacheService.getCache(resourceName).get(value.identifier)

    private fun ResponseFintEvent.isError(): Boolean = !(isFailed || isRejected || isConflicted)

    private fun ResponseFintEvent.convertResource(resourceName: String): FintResource? = resourceConverter.convert(resourceName, value)

    private fun ResponseFintEvent.toStatus(type: OperationState) = OperationStatus(type, EventBodyResponse.ofResponseEvent(this))

    private fun FintResource.toStatus() = OperationStatus(OperationState.CREATED, this, createSelfLinkUri())

    private fun FintResource.createSelfLinkUri() =
        selfLinks.first()?.let { URI.create(it.href) }
            ?: throw RuntimeException("Resource has no self link")
}
