package no.fintlabs.consumer.resource.event

import no.fintlabs.adapter.models.event.EventBodyResponse
import no.fintlabs.adapter.models.event.ResponseFintEvent
import org.springframework.http.HttpStatus
import java.net.URI

data class OperationStatus(
    val state: OperationState,
    val body: Any? = null,
    val location: URI? = null,
)

enum class OperationState(
    val status: HttpStatus,
) {
    ACCEPTED(HttpStatus.ACCEPTED), // 202: Request is still processing
    CREATED(HttpStatus.CREATED), // 201: Resource created successfully
    VALIDATED(HttpStatus.OK), // 200: Validation passed
    DELETED(HttpStatus.NO_CONTENT), // 204: Resource deleted
    GONE(HttpStatus.GONE), // 410: Request ID not found
    FAILED(HttpStatus.INTERNAL_SERVER_ERROR), // 500: Processing failed
    REJECTED(HttpStatus.BAD_REQUEST), // 400: Request invalid
    CONFLICT(HttpStatus.CONFLICT), // 409: Resource conflict
}

/**
 * Converts this [ResponseFintEvent] into an [OperationStatus] while preserving the legacy Core 1 response body structure.
 *
 * This is used for backwards compatibility. In Core 1, specific operation types (ERROR, REJECTED, and VALIDATE)
 * returned a wrapped `EventResponse` payload rather than the resource itself.
 *
 * **Mappings:**
 * - [OperationState.FAILED] -> Core 1 `ResponseStatus.ERROR`
 * - [OperationState.REJECTED] -> Core 1 `ResponseStatus.REJECTED`
 * - [OperationState.VALIDATED] -> Core 1 `Operation.VALIDATE`
 *
 * @param state The target operation state.
 * @return [OperationStatus] containing the [EventBodyResponse].
 * @throws IllegalArgumentException If the state does not support legacy body wrapping.
 */
fun ResponseFintEvent.toOperationStatusWithLegacyBody(state: OperationState): OperationStatus =
    when (state) {
        OperationState.FAILED,
        OperationState.REJECTED,
        OperationState.VALIDATED,
        -> {
            OperationStatus(state, EventBodyResponse.ofResponseEvent(this))
        }

        else -> {
            throw IllegalArgumentException(
                "Legacy body conversion failed: State '$state' is not supported. " +
                    "Core 1 compatibility only supports wrapping the raw EventResponse for states: [FAILED, REJECTED, VALIDATED].",
            )
        }
    }
