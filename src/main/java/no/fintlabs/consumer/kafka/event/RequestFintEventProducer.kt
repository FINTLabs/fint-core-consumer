package no.fintlabs.consumer.kafka.event

import com.fasterxml.jackson.databind.ObjectMapper
import java.time.Clock
import java.time.Duration
import java.util.UUID
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.EventCacheProperties
import no.fintlabs.consumer.resource.ResourceService
import no.novari.fint.model.resource.FintResource
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class RequestFintEventProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val eventTopicService: EventTopicService,
    private val config: ConsumerConfiguration,
    private val resourceService: ResourceService,
    private val objectMapper: ObjectMapper,
    private val props: EventCacheProperties,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val producer = parameterizedTemplateFactory.createTemplate(RequestFintEvent::class.java)
    private val ensuredTopics = mutableSetOf<String>()

    companion object {
        val RETENTION_TIME: Duration = Duration.ofDays(7)
        const val PARTITIONS = 1
    }

    fun sendEvent(
        resourceName: String,
        resourceData: Any?,
        operationType: OperationType,
    ): RequestFintEvent =
        resourceService
            .mapResourceAndLinks(resourceName, resourceData)
            .toEvent(resourceName, operationType)
            .also { event ->
                resourceName
                    .ensureTopic()
                    .run { publish(event, this) }
            }

    private fun FintResource.toEvent(
        resourceName: String,
        operationType: OperationType,
    ): RequestFintEvent =
        RequestFintEvent().apply {
            corrId = UUID.randomUUID().toString()
            orgId = config.orgId
            domainName = config.domain
            packageName = config.packageName
            this.resourceName = resourceName
            this.operationType = operationType
            created = clock.millis()
            timeToLive = created + props.getLifeCycleConfig(resourceName).ttl.toMillis()
            value = this@toEvent.toJson()
        }

    private fun String.ensureTopic(): EventTopicNameParameters =
        asEventName()
            .asEventTopic()
            .also { topic ->
                if (ensuredTopics.add(topic.eventName)) {
                    log.debug("Ensuring event topic: {}", topic.eventName)
                    eventTopicService.createOrModifyTopic(
                        topic,
                        EventTopicConfiguration
                            .stepBuilder()
                            .partitions(PARTITIONS)
                            .retentionTime(RETENTION_TIME)
                            .cleanupFrequency(EventCleanupFrequency.NORMAL)
                            .build(),
                    )
                }
            }

    private fun publish(
        event: RequestFintEvent,
        topic: EventTopicNameParameters,
    ) = producer.send(
        ParameterizedProducerRecord
            .builder<RequestFintEvent>()
            .key(event.corrId)
            .topicNameParameters(topic)
            .value(event)
            .build(),
    )

    private fun String.asEventName(): String = "${config.domain}-${config.packageName}-$this-request"

    private fun String.asEventTopic(): EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId(config.orgId.toTopicFormat())
                    .domainContextApplicationDefault()
                    .build(),
            )
            .eventName(this)
            .build()

    private fun Any.toJson(): String = objectMapper.writeValueAsString(this)

    private fun String.toTopicFormat() = replace(".", "-")
}
