package no.fintlabs.consumer.kafka.entity

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.relation.RelationService
import no.fintlabs.kafka.entity.EntityConsumerConfiguration
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
open class RelationUpdateConsumer(
    private val relationService: RelationService,
    private val consumerConfig: ConsumerConfiguration,
) {
    @Bean
    @ConditionalOnProperty(
        name = ["fint.consumer.autorelation"],
        havingValue = "true",
        matchIfMissing = true,
    )
    open fun relationUpdateConsumerContainer(
        consumerFactoryService: EntityConsumerFactoryService,
    ): ConcurrentMessageListenerContainer<String?, RelationUpdate> =
        consumerFactoryService
            .createFactory(
                RelationUpdate::class.java,
                this::consumeRecord,
                EntityConsumerConfiguration
                    .builder()
                    .seekingOffsetResetOnAssignment(true)
                    .build(),
            ).createContainer(
                EntityTopicNameParameters
                    .builder()
                    .orgId(consumerConfig.orgId.toTopicFormat())
                    .domainContext("fint-core")
                    .resource("relation-update")
                    .build(),
            )

    fun consumeRecord(consumerRecord: ConsumerRecord<String, RelationUpdate>) =
        consumerRecord
            .value()
            .takeIf { it.belongsToThisService() }
            ?.let { relationService.processRelationUpdate(it) }

    private fun RelationUpdate.belongsToThisService() =
        with(targetEntity) {
            consumerConfig.matchesComponent(domainName, packageName)
        }

    private fun String.toTopicFormat() = replace(".", "-")
}
