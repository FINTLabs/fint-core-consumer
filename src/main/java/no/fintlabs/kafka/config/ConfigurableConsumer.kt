package no.fintlabs.kafka.config

import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.listener.AbstractConsumerSeekAware
import org.springframework.kafka.listener.ConsumerSeekAware

abstract class ConfigurableConsumer(
    private val kafkaProperties: KafkaProperties,
    private val consumerName: String,
) : AbstractConsumerSeekAware() {
    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback,
    ) {
        if (kafkaProperties.consumers[consumerName]?.seekToBeginning == true) {
            assignments.keys.forEach { callback.seekToBeginning(it.topic(), it.partition()) }
        }
    }
}
