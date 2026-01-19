package no.fintlabs.autorelation

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.entity.EntityConsumerConfiguration
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer

@Configuration
class AutoRelationEntityConsumer(
    private val consumerConfig: ConsumerConfiguration,
    private val autoRelationService: AutoRelationService,
) {
    @Bean
    fun buildAutoRelationConsumer(
        consumerFactoryService: EntityConsumerFactoryService,
    ): ConcurrentMessageListenerContainer<String, in Any> =
        consumerFactoryService
            .createFactory(
                Any::class.java,
                this::consumeRecord,
                EntityConsumerConfiguration
                    .builder()
                    .groupIdSuffix("autorelation")
                    .seekingOffsetResetOnAssignment(false)
                    .build(),
            ).createContainer(
                EntityTopicNamePatternParameters
                    .builder()
                    .orgId(FormattedTopicComponentPattern.anyOf(createOrgId()))
                    .domainContext(FormattedTopicComponentPattern.anyOf("fint-core"))
                    .resource(FormattedTopicComponentPattern.startingWith(createResourcePattern()))
                    .build(),
            )

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any>) =
        autoRelationService.handleNewEntity(consumerRecord.resourceName(), consumerRecord.key(), consumerRecord.value())

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-")

    private fun createResourcePattern() = "${consumerConfig.domain}-${consumerConfig.packageName}"

    private fun ConsumerRecord<String, in Any>.resourceName(): String = topic().substringAfterLast("-")
}
