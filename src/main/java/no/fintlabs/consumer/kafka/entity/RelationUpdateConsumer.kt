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
    ): ConcurrentMessageListenerContainer<String, RelationUpdate> =
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
            .takeIf { it.belongsToThisService() }
            ?.let { relationService.applyOrBufferUpdate(it) }

    private fun RelationUpdate.belongsToThisService() =
        consumerConfig.matchesConfiguration(targetEntity.domainName, targetEntity.packageName, orgId)
}
