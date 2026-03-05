package no.fintlabs.cache

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import no.fintlabs.autorelation.model.createDeleteRequest
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val cacheResourceLockService: CacheResourceLockService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer,
    private val meterRegistry: MeterRegistry,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val evictionStates = ConcurrentHashMap<String, EvictionState>()

    @Async("evictionTaskExecutor")
    fun evictExpired(resourceName: String) {
        val state = evictionStates.computeIfAbsent(resourceName) { EvictionState() }

        state.pending.set(true)
        if (!state.running.compareAndSet(false, true)) {
            logger.info(
                "Cache eviction already running, queued a new run: org={}, resource={}",
                consumerConfig.orgId,
                safeResourceName(resourceName),
            )
            return
        }

        do {
            state.pending.set(false)
            try {
                evictExpiredInternal(resourceName)
            } catch (runtimeException: RuntimeException) {
                logger.error(
                    "Async cache eviction failed: org={}, resource={}",
                    consumerConfig.orgId,
                    safeResourceName(resourceName),
                    runtimeException,
                )
            }
            state.running.set(false)
        } while (state.pending.get() && state.running.compareAndSet(false, true))
    }

    private fun evictExpiredInternal(resourceName: String) {
        cacheResourceLockService.withLock(resourceName) {
            timed(resourceName, "eviction.total") {
                timed(resourceName, "eviction.cache.lookup") {
                    cacheService.getCache(resourceName)
                }?.let { cache ->
                    timed(resourceName, "eviction.cache.evictOldCacheObjects") {
                        cache.evictOldCacheObjects { _, cacheObject ->
                            timed(resourceName, "eviction.publishDeleteRequest") {
                                onCacheEviction(resourceName, cacheObject.unboxObject())
                            }
                        }
                    }
                }
            }
        }
    }

    private fun onCacheEviction(
        resource: String,
        resourceObject: Any,
    ) = relationRequestProducer.publish(
        createDeleteRequest(
            consumerConfig.orgId,
            consumerConfig.domain,
            consumerConfig.packageName,
            resource,
            resourceObject,
        ),
    )

    private fun <T> timed(
        resourceName: String,
        operation: String,
        supplier: () -> T,
    ): T {
        val sample = Timer.start(meterRegistry)
        var status = "success"
        return try {
            supplier.invoke()
        } catch (runtimeException: RuntimeException) {
            status = "error"
            throw runtimeException
        } finally {
            sample.stop(timer(resourceName, operation, status))
        }
    }

    private fun timer(
        resourceName: String,
        operation: String,
        status: String,
    ): Timer =
        Timer
            .builder("core.consumer.eviction.processing")
            .description("Duration of cache eviction processing steps")
            .tag("org", consumerConfig.orgId)
            .tag("resource", safeResourceName(resourceName))
            .tag("operation", operation)
            .tag("status", status)
            .register(meterRegistry)

    private fun safeResourceName(resourceName: String?): String = resourceName?.takeIf { it.isNotBlank() } ?: "unknown"

    private class EvictionState(
        val running: AtomicBoolean = AtomicBoolean(false),
        val pending: AtomicBoolean = AtomicBoolean(false),
    )
}
