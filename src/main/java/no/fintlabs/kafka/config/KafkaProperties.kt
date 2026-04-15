package no.fintlabs.kafka.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("fint.kafka")
data class KafkaProperties(
    val consumers: Map<String, ConsumerProperties> = emptyMap(),
) {
    data class ConsumerProperties(
        val seekToBeginning: Boolean = false,
        val concurrency: Int = 1,
    )
}
