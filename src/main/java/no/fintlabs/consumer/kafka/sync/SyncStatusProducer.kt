package no.fintlabs.consumer.kafka.sync

import no.fintlabs.consumer.kafka.sync.model.SyncStatus
import no.fintlabs.kafka.event.EventProducerFactory
import no.fintlabs.kafka.event.EventProducerRecord
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.kafka.event.topic.EventTopicService
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class SyncStatusProducer(
    eventProducerFactory: EventProducerFactory,
    eventTopicService: EventTopicService,
) {
    private val eventProducer = eventProducerFactory.createProducer(SyncStatus::class.java)
    private val eventTopic = createEventTopic()

    init {
        eventTopicService.ensureTopic(eventTopic, Duration.ofDays(7).toMillis())
    }

    fun publish(syncStatus: SyncStatus) =
        eventProducer.send(
            EventProducerRecord
                .builder<SyncStatus>()
                .topicNameParameters(eventTopic)
                .value(syncStatus)
                .build(),
        )

    private fun createEventTopic() =
        EventTopicNameParameters
            .builder()
            .orgId("fintlabs-no")
            .domainContext("fint-core")
            .eventName("sync-status")
            .build()
}
