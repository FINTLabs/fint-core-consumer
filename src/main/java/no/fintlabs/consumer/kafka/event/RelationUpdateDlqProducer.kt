package no.fintlabs.consumer.kafka.event

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicService
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class RelationUpdateDlqProducer(
    eventProducerFactory: EventProducerFactory,
    eventTopicService: EventTopicService
) {

    private val eventProducer = eventProducerFactory.createProducer(RelationUpdate::class.java)
    private val eventTopic = createEventTopic()

    init {
        eventTopicService.ensureTopic(eventTopic, Duration.ofHours(3).toMillis())
    }

    fun publish(relationUpdate: RelationUpdate) =
        eventProducer.send(
            EventProducerRecord.builder<RelationUpdate>()
                .topicNameParameters(eventTopic)
                .value(relationUpdate)
                .build()
        )

    private fun createEventTopic() =
        EventTopicNameParameters.builder()
            .orgId("fintlabs-no")
            .domainContext("fint-core")
            .eventName("relation-update-dlq")
            .build()

}