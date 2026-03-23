package no.fintlabs.consumer.health

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "fint.consumer.health.kafka")
data class KafkaHealthProperties(
    val idleEventInterval: Duration = Duration.ofMinutes(1),
    val runtimeGracePeriod: Duration = Duration.ofMinutes(15),
    val monitorIntervalSeconds: Int = 30,
    val noPollThreshold: Float = 3.0f,
)
