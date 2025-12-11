package no.fintlabs.consumer.resource

import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.EndpointsConstants
import no.fintlabs.consumer.kafka.event.RequestFintEventProducer
import no.fintlabs.consumer.resource.aspect.IdFieldCheck
import no.fintlabs.consumer.resource.aspect.WriteableResource
import no.fintlabs.consumer.resource.dto.LastUpdatedResponse
import no.fintlabs.consumer.resource.dto.ResourceCacheSizeResponse
import no.fintlabs.consumer.resource.event.OperationStatus
import no.fintlabs.consumer.resource.event.RequestStatusService
import no.fintlabs.model.resource.FintResources
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.net.URI
import kotlin.jvm.optionals.getOrNull

@RestController
@RequestMapping("/{resource}")
@RequiredArgsConstructor
@Slf4j
class ResourceController(
    private val resourceService: ResourceService,
    private val requestFintEventProducer: RequestFintEventProducer,
    private val requestStatusService: RequestStatusService,
    private val consumerConfig: ConsumerConfiguration,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @GetMapping
    fun getResource(
        @PathVariable resource: String,
        @RequestParam(defaultValue = "0") size: Int,
        @RequestParam(defaultValue = "0") offset: Int,
        @RequestParam(defaultValue = "0") sinceTimeStamp: Long,
        @RequestParam(required = false, name = "\$filter") filter: String?,
    ): FintResources? =
        resourceService.getResources(
            resource.lowercase(),
            size,
            offset,
            sinceTimeStamp,
            filter,
        )

    @PostMapping("/\$query")
    fun getResourceByOdataFilter(
        @PathVariable resource: String,
        @RequestParam(defaultValue = "0") size: Int,
        @RequestParam(defaultValue = "0") offset: Int,
        @RequestParam(defaultValue = "0") sinceTimeStamp: Long,
        @RequestBody(required = false) filter: String?,
    ): FintResources? = getResource(resource, size, offset, sinceTimeStamp, filter)

    @IdFieldCheck
    @GetMapping(EndpointsConstants.BY_ID)
    fun getResourceById(
        @PathVariable resource: String,
        @PathVariable idField: String?,
        @PathVariable idValue: String,
    ): ResponseEntity<FintResource?> =
        resourceService
            .getResourceById(resource.lowercase(), idField, idValue)
            .getOrNull()
            ?.let { ResponseEntity.ok(it) }
            ?: ResponseEntity.notFound().build()

    @GetMapping(EndpointsConstants.LAST_UPDATED)
    fun getLastUpdated(
        @PathVariable resource: String,
    ): ResponseEntity<LastUpdatedResponse> =
        resourceService.getLastUpdated(resource).let {
            ResponseEntity.ok(LastUpdatedResponse(it))
        }

    @GetMapping(EndpointsConstants.CACHE_SIZE)
    fun getResourceCacheSize(
        @PathVariable resource: String,
    ): ResponseEntity<ResourceCacheSizeResponse> =
        resourceService.getCacheSize(resource).let {
            ResponseEntity.ok(ResourceCacheSizeResponse(it))
        }

    @WriteableResource
    @GetMapping(EndpointsConstants.STATUS_ID)
    fun getStatus(
        @PathVariable resource: String,
        @PathVariable corrId: String,
    ): ResponseEntity<Any?> =
        requestStatusService
            .getStatusResponse(resource, corrId)
            .also { logger.info("Event: $corrId returned ${it.state.status}") }
            .toResponse()

    @WriteableResource
    @PostMapping
    fun postResource(
        @PathVariable resource: String,
        @RequestBody resourceData: Any,
        @RequestParam(name = "validate", required = false) validate: Boolean,
    ): ResponseEntity<Nothing> =
        requestFintEventProducer
            .sendEvent(resource.lowercase(), resourceData, validate.getOperationType())
            .toAcceptedResponse()

    @IdFieldCheck
    @WriteableResource
    @PutMapping(EndpointsConstants.BY_ID)
    fun putResource(
        @PathVariable resource: String,
        @PathVariable idField: String,
        @PathVariable idValue: String,
        @RequestBody resourceData: Any?,
    ): ResponseEntity<Nothing> =
        requestFintEventProducer
            .sendEvent(resource.lowercase(), resourceData, OperationType.UPDATE)
            .toAcceptedResponse()

    private fun OperationStatus.toResponse() =
        location
            ?.let { ResponseEntity.status(state.status).location(it).body(body) }
            ?: ResponseEntity.status(state.status).body(body)

    private fun Boolean.getOperationType() = if (this) OperationType.VALIDATE else OperationType.CREATE

    private fun RequestFintEvent.toLocationUri(): URI = URI.create("${consumerConfig.componentUrl}/$resourceName/status/$corrId")

    private fun RequestFintEvent.toAcceptedResponse(): ResponseEntity<Nothing> = ResponseEntity.accepted().location(toLocationUri()).build()
}
