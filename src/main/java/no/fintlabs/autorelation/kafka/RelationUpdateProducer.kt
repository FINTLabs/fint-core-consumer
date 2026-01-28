package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicService
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.CompletableFuture

@Component
class RelationUpdateProducer(
    eventTopicService: EventTopicService,
    eventProducerFactory: EventProducerFactory,
    private val consumerConfiguration: ConsumerConfiguration,
) {
    companion object {
        /**
         * Retention time (7 days) matches the Core 2 maximum to ensure
         * relation updates do not expire before their associated resources.
         */
        const val RETENTION_TIME_IN_DAYS = 7L
    }

    private val eventTopic = createEventTopic()
    private val entityProducer = eventProducerFactory.createProducer(RelationUpdate::class.java)

    init {
        eventTopicService.ensureTopic(eventTopic, Duration.ofDays(RETENTION_TIME_IN_DAYS).toMillis())
    }

    fun publishRelationUpdate(relationUpdate: RelationUpdate): CompletableFuture<SendResult<String?, RelationUpdate>> =
        entityProducer.send(
            EventProducerRecord
                .builder<RelationUpdate>()
                .topicNameParameters(eventTopic)
                .value(relationUpdate)
                .build(),
        )

    private fun createEventTopic() =
        EventTopicNameParameters
            .builder()
            .orgId(consumerConfiguration.orgId.toTopicFormat())
            .domainContext("fint-core")
            .eventName("relation-update")
            .build()

    private fun String.toTopicFormat() = replace(".", "-")
}
