package no.fintlabs.consumer.health

import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Component

@Component
class KafkaListenerContainerHealthConfigurer(
    private val kafkaHealthProperties: KafkaHealthProperties,
) {
    fun <VALUE> customize(container: ConcurrentMessageListenerContainer<String, VALUE>) {
        container.containerProperties.idleEventInterval = kafkaHealthProperties.idleEventInterval.toMillis()
        container.containerProperties.monitorInterval = kafkaHealthProperties.monitorIntervalSeconds
        container.containerProperties.noPollThreshold = kafkaHealthProperties.noPollThreshold
    }
}
