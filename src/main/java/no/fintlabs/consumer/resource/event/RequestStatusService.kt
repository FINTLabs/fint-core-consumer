package no.fintlabs.consumer.resource.event

import lombok.extern.slf4j.Slf4j
import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.event.EventBodyResponse
import no.fintlabs.adapter.models.event.ResponseFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceConverter
import org.springframework.stereotype.Service
import java.net.URI

/**
 * Service responsible for the lifecycle of a user request.
 *
 * It acts as the bridge between the event-driven backend and the synchronous HTTP API,
 * ensuring that responses (Created, Updated) are only returned when the data is
 * guaranteed to be consistent in the cache.
 */
@Slf4j
@Service
class RequestStatusService(
    private val eventService: EventService,
    private val cacheService: CacheService,
    private val resourceConverter: ResourceConverter,
    private val linkService: LinkService,
) {
    /**
     * Checks the status of a request given its Correlation ID.
     *
     * @return [OperationStatus] containing the state (e.g. ACCEPTED, CREATED, FAILED)
     * and the resource body if available.
     */
    fun getStatusResponse(
        resourceName: String,
        corrId: String,
    ): OperationStatus =
        eventService
            .getResponse(corrId)
            ?.let { handleFinishedEvent(resourceName, it) }
            ?: handleUnknownOrRunningEvent(corrId)

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
            OperationType.VALIDATE -> response.toOperationStatusWithLegacyBody(OperationState.VALIDATED)

            OperationType.DELETE -> OperationStatus(OperationState.DELETED)

            // For Create/Update, we must verify the Cache contains the new data before confirming 201 Created
            OperationType.CREATE,
            OperationType.UPDATE,
            -> ensureCacheConsistency(resourceName, response)
        }

    /**
     * Fetches the resource if the Cache has "caught up" with the Event processing.
     * We compare the timestamp of the data in the cache vs. when the adapter handled the event.
     * Ref: https://github.com/FINTLabs/novari-architecture-documentation/blob/main/Core/v2/status_handling.md
     */
    private fun ensureCacheConsistency(
        resourceName: String,
        responseFintEvent: ResponseFintEvent,
    ): OperationStatus =
        responseFintEvent
            .fetchConsistentResource(resourceName)
            ?.let { OperationStatus(OperationState.CREATED, it, it.createSelfLinkUri()) }
            ?: handleUnknownOrRunningEvent(responseFintEvent.corrId)

    fun handleErrorResponse(
        resourceName: String,
        responseFintEvent: ResponseFintEvent,
    ): OperationStatus =
        if (responseFintEvent.isFailed) {
            responseFintEvent.toOperationStatusWithLegacyBody(OperationState.FAILED)
        } else if (responseFintEvent.isRejected) {
            responseFintEvent.toOperationStatusWithLegacyBody(OperationState.REJECTED)
        } else if (responseFintEvent.isConflicted) {
            OperationStatus(OperationState.CONFLICT, responseFintEvent.convertResourceAndMapLinks(resourceName))
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
     * Retrieves the cached resource only if its timestamp matches this event's [ResponseFintEvent.handledAt].
     *
     * This relies on the Provider updating the cache using the event's `handledAt` time,
     * confirming that the data in the cache is the direct result of this specific operation.
     */
    private fun ResponseFintEvent.fetchConsistentResource(resourceName: String): FintResource? {
        val cache = cacheService.getCache(resourceName)
        val cacheTimestamp = cache.getLastDelivered(value.identifier)

        return takeIf { cacheTimestamp == handledAt }
            ?.let { cache.get(value.identifier) }
    }

    private fun ResponseFintEvent.isError(): Boolean = isFailed || isRejected || isConflicted

    private fun ResponseFintEvent.convertResourceAndMapLinks(resourceName: String): FintResource? =
        resourceConverter
            .convert(resourceName, value.resource)
            .also { linkService.mapLinks(resourceName, it) }

    private fun FintResource.createSelfLinkUri() =
        selfLinks.firstOrNull()?.let { URI.create(it.href) }
            ?: throw RuntimeException("Resource has no self link")
}
