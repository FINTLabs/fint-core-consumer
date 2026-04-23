package no.fintlabs.consumer.kafka

import no.fintlabs.consumer.config.ConsumerConfiguration
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationContext
import org.springframework.context.event.EventListener
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.stereotype.Component
import java.util.concurrent.ThreadLocalRandom

@Component
class KafkaListenerStartupJitter(
    private val consumerConfig: ConsumerConfiguration,
    private val applicationContext: ApplicationContext,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaListenerStartupJitter::class.java)
    }

    @EventListener(ApplicationReadyEvent::class)
    fun startListenersAfterJitter() {
        val jitter = consumerConfig.kafka.startupJitter
        if (jitter.isZero) return

        val containers = applicationContext.getBeansOfType(MessageListenerContainer::class.java).values
        val delayMs = ThreadLocalRandom.current().nextLong(jitter.toMillis() + 1)

        logger.info(
            "Delaying Kafka listener startup by {} ms (max jitter {} ms) across {} containers",
            delayMs,
            jitter.toMillis(),
            containers.size,
        )

        Thread.ofVirtual().name("kafka-startup-jitter").start {
            try {
                Thread.sleep(delayMs)
            } catch (ex: InterruptedException) {
                Thread.currentThread().interrupt()
                logger.warn("Kafka startup jitter interrupted; starting containers immediately")
            }
            containers.forEach { container ->
                if (!container.isRunning) {
                    logger.info("Starting Kafka listener container: {}", container.listenerId ?: container)
                    container.start()
                }
            }
        }
    }
}
