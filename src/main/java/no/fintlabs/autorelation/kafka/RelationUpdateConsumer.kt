package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.AutoRelationService
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.event.EventConsumerConfiguration
import no.fintlabs.kafka.event.EventConsumerFactoryService
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class RelationUpdateConsumer(
    private val autoRelationService: AutoRelationService,
    private val consumerConfig: ConsumerConfiguration,
) {
    @Bean
    @ConditionalOnProperty(
        name = ["fint.consumer.autorelation"],
        havingValue = "true",
        matchIfMissing = true,
    )
    fun relationUpdateConsumerContainer(
        consumerFactoryService: EventConsumerFactoryService,
    ): ConcurrentMessageListenerContainer<String?, RelationUpdate> =
        consumerFactoryService
            .createFactory(
                RelationUpdate::class.java,
                this::consumeRecord,
                EventConsumerConfiguration
                    .builder()
                    .seekingOffsetResetOnAssignment(false)
                    .build(),
            ).createContainer(
                EventTopicNameParameters
                    .builder()
                    .orgId(consumerConfig.orgId.toTopicFormat())
                    .domainContext("fint-core")
                    .eventName("relation-update")
                    .build(),
            )

    fun consumeRecord(consumerRecord: ConsumerRecord<String?, RelationUpdate>) =
        consumerRecord
            .value()
            .takeIf { it.belongsToThisService() }
            ?.run { autoRelationService.applyOrBufferUpdate(this) }

    private fun RelationUpdate.belongsToThisService() =
        with(targetEntity) {
            consumerConfig.matchesComponent(domainName, packageName)
        }

    private fun String.toTopicFormat() = replace(".", "-")
}
