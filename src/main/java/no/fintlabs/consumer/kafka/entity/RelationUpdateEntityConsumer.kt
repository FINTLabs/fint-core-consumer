package no.fintlabs.consumer.kafka.entity

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.links.relation.RelationService
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

@Component
class RelationUpdateEntityConsumer(
    private val relationService: RelationService
) {

    @Bean
    fun entityConsumerFactory(consumerFactoryService: EntityConsumerFactoryService) =
        consumerFactoryService
            .createFactory(RelationUpdate::class.java, this::consumeRecord)
            .createContainer(
                EntityTopicNameParameters.builder()
                    .orgId("fintlabs-no")
                    .domainContext("fint-core")
                    .resource("relation-update")
                    .build()
            )

    fun consumeRecord(consumerRecord: ConsumerRecord<String, RelationUpdate>) =
        relationService.processIfApplicable(consumerRecord.value())

}