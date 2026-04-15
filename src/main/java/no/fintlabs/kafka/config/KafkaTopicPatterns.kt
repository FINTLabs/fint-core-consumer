package no.fintlabs.kafka.config

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.KafkaTopicName
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class KafkaTopicPatterns(
    private val consumerConfig: ConsumerConfiguration,
) {
    private val component: String
        get() = "${consumerConfig.domain}-${consumerConfig.packageName}"

    @Bean
    fun resourceTopicPattern(): String =
        KafkaTopicName.entity(
            consumerConfig.orgId,
            component,
        )

    @Bean
    fun relationUpdateTopicPattern(): String =
        KafkaTopicName.entity(
            consumerConfig.orgId,
            "$component-relation-update",
        )

    @Bean
    fun eventRequestTopicPattern(): String =
        KafkaTopicName.event(
            consumerConfig.orgId,
            "$component-request",
        )

    @Bean
    fun eventResponseTopicPattern(): String =
        KafkaTopicName.event(
            consumerConfig.orgId,
            "$component-response",
        )
}
