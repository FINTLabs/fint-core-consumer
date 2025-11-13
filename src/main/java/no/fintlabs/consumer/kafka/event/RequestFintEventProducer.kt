package no.fintlabs.consumer.kafka.event

import com.fasterxml.jackson.databind.ObjectMapper
import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.ResourceService
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

@Service
class RequestFintEventProducer(
    eventProducerFactory: EventProducerFactory,
    private val eventTopicService: EventTopicService,
    private val config: ConsumerConfiguration,
    private val resourceService: ResourceService,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val producer = eventProducerFactory.createProducer(RequestFintEvent::class.java)
    private val ensuredTopics = mutableSetOf<String>()
    private val retention = 7.days

    fun sendEvent(
        resourceName: String,
        resourceData: Any,
        validate: Boolean,
    ): RequestFintEvent =
        resourceService
            .mapResourceAndLinks(resourceName, resourceData)
            .toEvent(resourceName, validate)
            .also { event ->
                resourceName
                    .ensureTopic()
                    .run { publish(event, this) }
            }

    private fun FintResource.toEvent(
        resourceName: String,
        validate: Boolean,
    ): RequestFintEvent =
        RequestFintEvent().apply {
            corrId = UUID.randomUUID().toString()
            orgId = config.orgId
            domainName = config.domain
            packageName = config.packageName
            this.resourceName = resourceName
            operationType = if (validate) OperationType.VALIDATE else OperationType.UPDATE
            created = System.currentTimeMillis()
            timeToLive = 30.minutes.inWholeMilliseconds
            value = this@toEvent.toJson()
        }

    private fun String.ensureTopic(): EventTopicNameParameters =
        asEventName()
            .asEventTopic()
            .also { topic ->
                if (ensuredTopics.add(topic.eventName)) {
                    log.debug("Ensuring event topic: {}", topic.eventName)
                    eventTopicService.ensureTopic(topic, retention.toJavaDuration().toMillis())
                }
            }

    private fun publish(
        event: RequestFintEvent,
        topic: EventTopicNameParameters,
    ) = producer.send(
        EventProducerRecord
            .builder<RequestFintEvent>()
            .key(event.corrId)
            .topicNameParameters(topic)
            .value(event)
            .build(),
    )

    private fun String.asEventName(): String = "${config.domain}-${config.packageName}-$this"

    private fun String.asEventTopic(): EventTopicNameParameters = EventTopicNameParameters.builder().eventName(this).build()

    private fun Any.toJson(): String = objectMapper.writeValueAsString(this)
}
