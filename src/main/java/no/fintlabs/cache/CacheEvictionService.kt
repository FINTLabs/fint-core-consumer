package no.fintlabs.cache

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import no.fintlabs.autorelation.model.createDeleteRequest
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import org.springframework.stereotype.Service

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer,
    private val meterRegistry: MeterRegistry,
) {
    fun evictExpired(resourceName: String) =
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
}
