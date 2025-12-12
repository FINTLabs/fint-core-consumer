package no.fintlabs.consumer.kafka.entity

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.relation.RelationService
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
open class RelationUpdateConsumer(
    private val relationService: RelationService,
    private val consumerConfig: ConsumerConfiguration,
) {
    @Bean
    @ConditionalOnProperty(name = ["fint.consumer.autorelation"], havingValue = "true")
    open fun relationUpdateConsumerContainer(consumerFactoryService: EntityConsumerFactoryService) =
        consumerFactoryService
            .createFactory(RelationUpdate::class.java, this::consumeRecord)
            .createContainer(
                EntityTopicNameParameters
                    .builder()
                    .orgId("fintlabs-no")
                    .domainContext("fint-core")
                    .resource("relation-update")
                    .build(),
            )

    fun consumeRecord(consumerRecord: ConsumerRecord<String, RelationUpdate>) =
        consumerRecord
            .value()
            .takeIf { belongsToThisService(it) }
            ?.let { relationService.processRelationUpdate(it) }

    private fun belongsToThisService(relationUpdate: RelationUpdate) =
        consumerConfig.matchesConfiguration(relationUpdate.domainName, relationUpdate.packageName, relationUpdate.orgId)
}
