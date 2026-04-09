package no.fintlabs.kafka.config

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.KafkaTopicName
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class KafkaTopicPatterns(
    private val consumerConfig: ConsumerConfiguration,
) {
    @Bean
    fun resourceTopicPattern(): String =
        KafkaTopicName.entity(
            consumerConfig.orgId,
            "${consumerConfig.domain}-${consumerConfig.packageName}",
        )

    @Bean
    fun relationUpdateTopicPattern(): String =
        KafkaTopicName.entity(
            consumerConfig.orgId,
            "${consumerConfig.domain}-${consumerConfig.packageName}-relation-update",
        )
}
