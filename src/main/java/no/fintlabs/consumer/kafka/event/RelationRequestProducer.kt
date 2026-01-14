package no.fintlabs.consumer.kafka.event

import no.fintlabs.autorelation.model.RelationEvent
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicService
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.CompletableFuture

@Component
class RelationRequestProducer(
    eventTopicService: EventTopicService,
    eventProducerFactory: EventProducerFactory,
) {
    private val eventProducer = eventProducerFactory.createProducer(RelationEvent::class.java)
    private val eventTopic = createEventTopic()

    init {
        eventTopicService.ensureTopic(eventTopic, Duration.ofHours(3).toMillis())
    }

    fun publish(relationRequest: RelationEvent): CompletableFuture<SendResult<String?, RelationEvent>> =
        eventProducer.send(
            EventProducerRecord
                .builder<RelationEvent>()
                .topicNameParameters(eventTopic)
                .value(relationRequest)
                .build(),
        )

    private fun createEventTopic() =
        EventTopicNameParameters
            .builder()
            .orgId("fintlabs-no")
            .domainContext("fint-core")
            .eventName("relation-request")
            .build()
}
