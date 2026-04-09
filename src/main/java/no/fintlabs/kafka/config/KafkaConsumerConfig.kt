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
    fun resourceFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> =
        createFactory(kafkaProperties.consumers[KafkaConsumerNames.RESOURCE])

    @Bean
    fun autoRelationResourceFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> =
        createFactory(kafkaProperties.consumers[KafkaConsumerNames.AUTO_RELATION_RESOURCE])

    @Bean
    fun relationUpdateFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> =
        createFactory(kafkaProperties.consumers[KafkaConsumerNames.RELATION_UPDATE])

    private fun createFactory(
        props: KafkaProperties.ConsumerProperties?,
    ): ConcurrentKafkaListenerContainerFactory<String, Any> =
        ConcurrentKafkaListenerContainerFactory<String, Any>().apply {
            consumerFactory = this@KafkaConsumerConfig.consumerFactory
            setConcurrency(props?.concurrency ?: 1)
        }
}
