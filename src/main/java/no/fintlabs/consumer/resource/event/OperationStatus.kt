package no.fintlabs.consumer.resource.event

import org.springframework.http.HttpStatus
import java.net.URI

data class OperationStatus(
    val type: OperationState,
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
