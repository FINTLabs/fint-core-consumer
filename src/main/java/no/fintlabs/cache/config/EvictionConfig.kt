package no.fintlabs.cache.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("fint.consumer.cache.eviction")
data class EvictionConfig(
    val evictionDelay: Duration = Duration.ofMinutes(10),
    val acceptanceWindow: Duration = Duration.ofMinutes(10)
)