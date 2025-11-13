package no.fintlabs.consumer.resource

import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import no.fint.model.resource.FintResource
import no.fint.model.resource.FintResources
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.EndpointsConstants
import no.fintlabs.consumer.kafka.event.RequestFintEventProducer
import no.fintlabs.consumer.resource.aspect.IdFieldCheck
import no.fintlabs.consumer.resource.aspect.WriteableResource
import no.fintlabs.consumer.resource.event.EventStatusService
import org.springframework.http.HttpHeaders
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.net.URI
import java.util.*
import kotlin.jvm.optionals.getOrNull

@RestController
@RequestMapping("/{resource}")
@RequiredArgsConstructor
@Slf4j
class ResourceController(
    private val resourceService: ResourceService,
    private val requestFintEventProducer: RequestFintEventProducer,
    private val eventStatusService: EventStatusService,
) {
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
        eventStatusService.getStatusResponse(resource, corrId).let { statusResponse ->
            statusResponse.location?.let {
                ResponseEntity.status(statusResponse.type.status).location(it).body(statusResponse.body)
            } ?: ResponseEntity.status(statusResponse.type.status).body(statusResponse.body)
        }

    @WriteableResource
    @PostMapping
    fun postResource(
        @PathVariable resource: String,
        @RequestBody resourceData: Any,
        @RequestParam(name = "validate", required = false) validate: Boolean,
    ): ResponseEntity<Nothing> =
        requestFintEventProducer
            .sendEvent(resource.lowercase(), resourceData, validate)
            .let { ResponseEntity.accepted().location(URI.create("asdf")).build() } // TODO: Set correct URI

    @IdFieldCheck
    @WriteableResource
    @PutMapping(EndpointsConstants.BY_ID)
    fun putResource(
        @PathVariable resource: String,
        @PathVariable idField: String,
        @PathVariable idValue: String,
        @RequestBody resourceData: Any?,
    ): ResponseEntity<Void?> {
        val requestFintEvent =
            requestFintEventProducer.sendEvent(resource.lowercase(Locale.getDefault()), resourceData, OperationType.UPDATE)
        return ResponseEntity
            .accepted()
            .header(HttpHeaders.LOCATION, eventStatusService.createStatusHref(requestFintEvent))
            .build<Void?>()
    }

}

data class LastUpdatedResponse(
    val lastUpdated: Long,
)

data class ResourceCacheSizeResponse(
    val size: Int,
)
