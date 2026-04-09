package no.fintlabs.consumer.exception.kafka

import no.fintlabs.consumer.config.OrgId
import no.fintlabs.kafka.KafkaTopicName
import no.fintlabs.status.models.error.ConsumerError
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class ConsumerErrorProducer(
    private val kafkaTemplate: KafkaTemplate<String, ConsumerError>,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val topic =
        KafkaTopicName.event(
            orgId = OrgId.from("fintlabs.no"),
            eventName = "consumer-error",
        )

    fun produce(consumerError: ConsumerError) {
        logger.info("Producing consumer-error to Kafka!")
        kafkaTemplate.send(topic, UUID.randomUUID().toString(), consumerError)
    }
}
