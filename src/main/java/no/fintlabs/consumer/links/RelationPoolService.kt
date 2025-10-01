package no.fintlabs.consumer.links

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.RelationPoolConfig
import no.fintlabs.consumer.kafka.event.RelationUpdateDlqProducer
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.pow

@Service
class RelationPoolService(
    private val poolConfig: RelationPoolConfig,
    private val relationService: RelationService,
    private val dlqProducer: RelationUpdateDlqProducer,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val queue = DelayQueue<RetryItem>()
    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "relation-pool-worker").apply { isDaemon = true }
    }

    private val worker = Runnable {
        logStartupConfig()

        while (!Thread.currentThread().isInterrupted) {
            runCatching {
                val item = queue.take()
                val (relationUpdate, attempt) = item.payload

                val success = relationService.processRelationUpdate(relationUpdate)
                handleProcessResult(success, relationUpdate, attempt)
            }.onFailure {
                when (it) {
                    is InterruptedException -> {
                        Thread.currentThread().interrupt()
                        logger.info("RelationPoolService worker interrupted, shutting down gracefully")
                    }

                    else -> logger.error("Unexpected error in RelationPoolService worker", it)
                }
            }
        }

        logger.warn("RelationPoolService worker stopped")
    }

    @EventListener
    fun onEnqueued(event: RelationUpdate) = enqueue(event)

    fun enqueue(relationUpdate: RelationUpdate, attempt: Int = 1) =
        queue.put(createRetryItem(relationUpdate, attempt))

    @PostConstruct
    fun start() {
        executor.submit(worker)
    }

    @PreDestroy
    fun stop() {
        executor.shutdownNow()
    }

    private fun handleProcessResult(success: Boolean, relationUpdate: RelationUpdate, attempt: Int) {
        if (success) {
            logger.debug(
                "Retry succeeded (attempt {}/{}): resource={}, relation={}",
                attempt, poolConfig.maxAttempts, relationUpdate.resource.name, relationUpdate.relation.name
            )
            return
        }

        if (attempt >= poolConfig.maxAttempts) {
            logger.debug(
                "Max attempts reached ({}). Hand off to DLQ/handler: resource={}, relation={}",
                poolConfig.maxAttempts, relationUpdate.resource.name, relationUpdate.relation.name
            )
            dlqProducer.publish(relationUpdate)
        } else {
            enqueue(relationUpdate, attempt + 1)
        }
    }

    private fun logStartupConfig() =
        logger.info(
            "RelationPoolService worker started (maxAttempts={}, initialDelaySeconds={}, exponentialBackoff={}, backoffMultiplier={})",
            poolConfig.maxAttempts,
            poolConfig.initialDelaySeconds,
            poolConfig.exponentialBackoff,
            poolConfig.backoffMultiplier
        )

    private fun createRetryItem(relationUpdate: RelationUpdate, attempt: Int = 1) =
        RetryItem(
            payload = Payload(relationUpdate, attempt),
            triggerTimeNanos = System.nanoTime() + computeDelay(attempt).toNanos()
        )

    private fun computeDelay(attempt: Int): Duration =
        if (poolConfig.exponentialBackoff) calculateExponentialDelaySeconds(attempt).let(Duration::ofSeconds)
        else Duration.ofSeconds(poolConfig.initialDelaySeconds)

    private fun calculateExponentialDelaySeconds(attempt: Int): Long =
        poolConfig.initialDelaySeconds
            .times(backoffFactor(attempt))
            .toLong()
            .coerceAtLeast(1L)

    private fun backoffFactor(attempt: Int): Double =
        poolConfig.backoffMultiplier.pow((attempt - 1).toDouble())

}

data class Payload(
    val relationUpdate: RelationUpdate,
    val attempt: Int
)

private data class RetryItem(
    val payload: Payload,
    private val triggerTimeNanos: Long
) : Delayed {
    override fun getDelay(unit: TimeUnit): Long {
        val remaining = triggerTimeNanos - System.nanoTime()
        return unit.convert(remaining, TimeUnit.NANOSECONDS)
    }

    override fun compareTo(other: Delayed): Int =
        this.getDelay(TimeUnit.NANOSECONDS).compareTo(other.getDelay(TimeUnit.NANOSECONDS))
}