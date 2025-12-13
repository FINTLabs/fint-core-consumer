package no.fintlabs.consumer.kafka.event

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.adapter.models.event.RequestFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceMapperService
import no.novari.kafka.producing.ParameterizedProducerRecord
import no.novari.kafka.producing.ParameterizedTemplateFactory
import no.novari.kafka.topic.EventTopicService
import no.novari.kafka.topic.configuration.EventCleanupFrequency
import no.novari.kafka.topic.configuration.EventTopicConfiguration
import no.novari.kafka.topic.name.EventTopicNameParameters
import no.novari.kafka.topic.name.TopicNamePrefixParameters
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Duration
import java.util.*

@Service
class EventProducer(
    parameterizedTemplateFactory: ParameterizedTemplateFactory,
    private val eventTopicService: EventTopicService,
    private val objectMapper: ObjectMapper,
    private val linkService: LinkService,
    private val resourceMapper: ResourceMapperService,
    private val consumerConfig: ConsumerConfiguration,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val eventProducer = parameterizedTemplateFactory.createTemplate(RequestFintEvent::class.java)
    private val logger = LoggerFactory.getLogger(javaClass)

    fun sendEvent(
        resourceName: String,
        resourceObject: Any,
        operationType: OperationType,
    ): RequestFintEvent =
        convertAndMapResource(
            resourceName,
            resourceObject,
        ) // TODO: Move this logic to own service, producer should only handle sending NOT mapping and converting
            .let { createRequestFintEvent(resourceName, it, operationType) }
            .also {
                createOrModifyTopic(resourceName)
                logger.info("Sending event: ${it.corrId}")
                eventProducer.send(
                    ParameterizedProducerRecord
                        .builder<RequestFintEvent>()
                        .topicNameParameters(createEventTopic(resourceName))
                        .value(it)
                        .build(),
                )
            }

    private fun convertAndMapResource(
        resourceName: String,
        resourceObject: Any,
    ): Any =
        resourceMapper
            .mapResource(resourceName, resourceObject)
            .also { linkService.mapLinks(resourceName, it) }

    private fun createRequestFintEvent(
        resourceName: String,
        resourceObject: Any,
        operationType: OperationType,
    ): RequestFintEvent =
        RequestFintEvent
            .builder()
            .corrId(UUID.randomUUID().toString())
            .domainName(consumerConfig.domain)
            .packageName(consumerConfig.packageName)
            .orgId(consumerConfig.orgId)
            .created(clock.millis())
            .resourceName(resourceName)
            .value(convertToJson(resourceObject))
            .operationType(operationType)
            .build()

    private fun createOrModifyTopic(resourceName: String) =
        eventTopicService.createOrModifyTopic(
            createEventTopic(resourceName),
            EventTopicConfiguration
                .stepBuilder()
                .partitions(1)
                .retentionTime(Duration.ofDays(7))
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )

    private fun convertToJson(resourceObject: Any): String {
        try {
            return objectMapper.writer().writeValueAsString(resourceObject)
        } catch (e: com.fasterxml.jackson.core.JsonProcessingException) {
            throw java.lang.RuntimeException(e)
        }
    }

    private fun createEventTopic(resourceName: String): EventTopicNameParameters =
        EventTopicNameParameters
            .builder()
            .topicNamePrefixParameters(
                TopicNamePrefixParameters
                    .stepBuilder()
                    .orgId(consumerConfig.orgId.toKafkaFormat())
                    .domainContextApplicationDefault()
                    .build(),
            ).eventName(createEventName(resourceName))
            .build()

    private fun String.toKafkaFormat() = this.replace(".", "-").lowercase()

    private fun createEventName(resourceName: String): String =
        "${consumerConfig.domain}-${consumerConfig.packageName}-$resourceName-request".lowercase()
}
