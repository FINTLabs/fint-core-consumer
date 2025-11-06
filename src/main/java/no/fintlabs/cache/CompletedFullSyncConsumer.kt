package no.fintlabs.cache

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.event.EventConsumerFactoryService
import no.fintlabs.kafka.event.topic.EventTopicNameParameters
import no.fintlabs.status.models.ResourceEvictionPayload
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

@Component
class CompletedFullSyncConsumer(
    private val consumerConfig: ConsumerConfiguration,
    private val cacheEvictionService: CacheEvictionService
) {

    @Bean
    fun completedFullSyncContainer(eventConsumerFactoryService: EventConsumerFactoryService) =
        eventConsumerFactoryService.createFactory(
            ResourceEvictionPayload::class.java,
            this::consume
        ).createContainer(createEventTopic())

    private fun consume(consumerRecord: ConsumerRecord<String?, ResourceEvictionPayload>) =
        consumerRecord.value().takeIf { consumerConfig.matchesConfiguration(it.domain, it.pkg, it.org) }
            ?.let { cacheEvictionService.triggerEviction(it) }

    private fun createEventTopic(): EventTopicNameParameters =
        EventTopicNameParameters.builder()
            .orgId("fintlabs-no")
            .domainContext("fint-core")
            .eventName("completed-full-sync")
            .build()
}