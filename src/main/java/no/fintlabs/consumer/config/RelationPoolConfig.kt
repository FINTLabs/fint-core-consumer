package no.fintlabs.consumer.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("relation.pool")
open class RelationPoolConfig {

    var maxAttempts: Int = 5
    var initialDelaySeconds: Long = 5
    var exponentialBackoff: Boolean = true
    var backoffMultiplier: Double = 2.0

}
