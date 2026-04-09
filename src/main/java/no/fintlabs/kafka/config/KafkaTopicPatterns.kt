package no.fintlabs.kafka.config

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.KafkaTopicNameConstants.ENTITY
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class KafkaTopicPatterns(
    private val consumerConfig: ConsumerConfiguration,
) {
    @Bean
    fun resourceTopicPattern(): String =
        "${consumerConfig.orgId.asTopicSegment}.fint-core.$ENTITY.${consumerConfig.domain}-${consumerConfig.packageName}"
}
