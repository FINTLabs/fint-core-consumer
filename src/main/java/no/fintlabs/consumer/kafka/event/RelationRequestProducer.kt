package no.fintlabs.consumer.kafka.event

import no.fintlabs.autorelation.kafka.model.RelationRequest
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicService
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class RelationRequestProducer(
    eventTopicService: EventTopicService,
    eventProducerFactory: EventProducerFactory
) {

    private val eventProducer = eventProducerFactory.createProducer(RelationRequest::class.java)
    private val eventTopic = createEventTopic()

    init {
        eventTopicService.ensureTopic(eventTopic, Duration.ofHours(3).toMillis())
    }

    fun publish(relationRequest: RelationRequest) =
        eventProducer.send(
            EventProducerRecord.builder<RelationRequest>()
                .topicNameParameters(eventTopic)
                .value(relationRequest)
                .build()
        )

    private fun createEventTopic() =
        EventTopicNameParameters.builder()
            .orgId("fintlabs-no")
            .domainContext("fint-core")
            .eventName("relation-request")
            .build()

}