package no.fintlabs.cache

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import no.fintlabs.autorelation.RelationEventService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.fint.model.resource.FintResource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val relationEventService: RelationEventService,
    private val consumerConfiguration: ConsumerConfiguration,
    private val meterRegistry: MeterRegistry,
) {
    fun evictExpired(
        resourceName: String,
        startTimestamp: Long,
    ) = timed(resourceName, "eviction.total") {
        val cache =
            timed(resourceName, "eviction.cache.lookup") {
                cacheService.getCache(resourceName)
            }
        timed(resourceName, "eviction.cache.evictExpired") {
            cache
                .evictExpired(startTimestamp)
                .forEach {
                    timed(resourceName, "eviction.relation.removeRelations") {
                        publishRelationDeleteRequest(resourceName, it.first, it.second)
                    }
                }
        }
    }

    private fun publishRelationDeleteRequest(
        resourceName: String,
        resourceId: String,
        resource: FintResource,
    ) = relationEventService.removeRelations(resourceName, resourceId, resource)

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
            logger.error(
                "Eviction component failed: operation={}, resource={}, org={}, status={}",
                operation,
                safeResourceName(resourceName),
                consumerConfiguration.orgId,
                status,
                runtimeException,
            )
            throw runtimeException
        } finally {
            val duration = Duration.ofNanos(sample.stop(timer(resourceName, operation, status)))
            if (duration > SLOW_COMPONENT_THRESHOLD) {
                logger.warn(
                    "Slow eviction component detected: operation={}, durationMs={}, resource={}, org={}, status={}",
                    operation,
                    duration.toMillis(),
                    safeResourceName(resourceName),
                    consumerConfiguration.orgId,
                    status,
                )
            }
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
            .tag("org", consumerConfiguration.orgId)
            .tag("resource", safeResourceName(resourceName))
            .tag("operation", operation)
            .tag("status", status)
            .register(meterRegistry)

    private fun safeResourceName(resourceName: String?): String = resourceName?.takeIf { it.isNotBlank() } ?: "unknown"

    companion object {
        private val logger = LoggerFactory.getLogger(CacheEvictionService::class.java)
        private val SLOW_COMPONENT_THRESHOLD = Duration.ofSeconds(10)
    }
}
