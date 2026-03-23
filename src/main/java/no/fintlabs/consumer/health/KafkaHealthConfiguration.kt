package no.fintlabs.consumer.health

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
class KafkaHealthConfiguration {
    @Bean
    fun kafkaHealthClock(): Clock = Clock.systemUTC()
}
