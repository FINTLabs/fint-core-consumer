package no.fintlabs.consumer.kafka.event

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.links.relation.RelationService
import no.fintlabs.kafka.event.EventConsumerFactoryService
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Component

@Component
class RelationUpdateEventConsumer(
    private val relationService: RelationService
) {

    @Bean
    fun relationUpdateConsumer(
        eventConsumerFactory: EventConsumerFactoryService,
    ): ConcurrentMessageListenerContainer<String?, RelationUpdate> =
        eventConsumerFactory.createFactory(
            RelationUpdate::class.java,
            this::consumeRecord,
        ).createContainer(
            EventTopicNameParameters.builder()
                .orgId("fintlabs-no")
                .domainContext("fint-core")
                .eventName("relation-update")
                .build()
        )


    fun consumeRecord(consumerRecord: ConsumerRecord<String, RelationUpdate>) =
        relationService.processIfApplicable(consumerRecord.value())


}