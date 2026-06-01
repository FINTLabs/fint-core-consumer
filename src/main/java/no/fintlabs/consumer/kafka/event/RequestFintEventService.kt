package no.fintlabs.consumer.kafka.event

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.EventCacheProperties
import no.fintlabs.consumer.resource.ResourceConverter
import no.fintlabs.consumer.resource.event.EventStatusStore
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service
import java.time.Clock
import java.util.UUID
import java.util.concurrent.TimeUnit

@Service
class RequestFintEventService(
    private val objectMapper: ObjectMapper,
    private val props: EventCacheProperties,
    private val config: ConsumerConfiguration,
    private val clock: Clock = Clock.systemUTC(),
    private val resourceConverter: ResourceConverter,
    private val requestFintEventProducer: RequestFintEventProducer,
    private val eventStatusStore: EventStatusStore,
) {
    fun createAndPublish(
        resourceName: String,
        resourceData: Any?,
        operationType: OperationType,
    ): RequestFintEvent =
        resourceData
            .toFintResource(resourceName)
            .toRequestFintEvent(resourceName, operationType)
            .also { event ->
                requestFintEventProducer.publish(event).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                eventStatusStore.storeRequest(event)
            }

    fun createAndPublish(
        resourceName: String,
        resourceData: Any?,
        validateOnly: Boolean = false,
    ): RequestFintEvent =
        validateOnly
            .toOperationType()
            .let { operationType -> createAndPublish(resourceName, resourceData, operationType) }

    private fun Boolean.toOperationType() = if (this) OperationType.VALIDATE else OperationType.CREATE

    private fun FintResource?.toRequestFintEvent(
        resourceName: String,
        operationType: OperationType,
    ) = RequestFintEvent().apply {
        corrId = UUID.randomUUID().toString()
        orgId = config.orgId.value
        domainName = config.domain
        packageName = config.packageName
        this.resourceName = resourceName
        this.operationType = operationType
        created = clock.millis()
        timeToLive = created + props.getLifeCycleConfig(resourceName).ttl.toMillis()
        value = this@toRequestFintEvent.toJson()
    }

    private fun Any?.toFintResource(resourceName: String) =
        this?.let {
            resourceConverter.convertAndMapLinks(resourceName, it)
        }

    private fun FintResource?.toJson() = objectMapper.writeValueAsString(this)

    companion object {
        private const val SEND_TIMEOUT_SECONDS = 10L
    }
}
