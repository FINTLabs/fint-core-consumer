package no.fintlabs.kafka

object KafkaTopicName {
    fun build(vararg segments: String): String = segments.joinToString(".")
}
