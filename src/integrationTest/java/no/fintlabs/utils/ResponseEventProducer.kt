package no.fintlabs.utils

import no.fintlabs.adapter.models.event.ResponseFintEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.KafkaTopicName
import org.springframework.boot.test.context.TestComponent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.util.concurrent.CompletableFuture

@TestComponent
class ResponseEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, ResponseFintEvent>,
    private val consumerConfig: ConsumerConfiguration,
) {
    private val topic =
        KafkaTopicName.event(
            orgId = consumerConfig.orgId,
            eventName = "${consumerConfig.domain}-${consumerConfig.packageName}-response",
        )

    fun produce(response: ResponseFintEvent): CompletableFuture<SendResult<String, ResponseFintEvent>> =
        kafkaTemplate.send(topic, response.corrId, response)
}
