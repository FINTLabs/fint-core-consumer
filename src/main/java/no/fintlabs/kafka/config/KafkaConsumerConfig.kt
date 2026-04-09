package no.fintlabs.kafka.config

import no.fintlabs.kafka.KafkaConsumerNames
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory

@Configuration
class KafkaConsumerConfig(
    private val kafkaProperties: KafkaProperties,
    private val consumerFactory: ConsumerFactory<String, Any>,
) {
    @Bean
    fun entityFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> =
        createFactory(kafkaProperties.consumers[KafkaConsumerNames.RESOURCE])

    @Bean
    fun autoRelationEntityFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> =
        createFactory(kafkaProperties.consumers[KafkaConsumerNames.AUTO_RELATION_RESOURCE])

    private fun createFactory(
        props: KafkaProperties.ConsumerProperties?,
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = consumerFactory
        factory.setConcurrency(props?.concurrency ?: 1)
        return factory
    }
}
