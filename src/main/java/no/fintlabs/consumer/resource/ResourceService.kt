package no.fintlabs.consumer.resource

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.model.resource.FintResources
import no.novari.fint.model.resource.FintResource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class ResourceService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val consumerConfiguration: ConsumerConfiguration,
    private val meterRegistry: MeterRegistry,
) {
    fun getResources(
        resourceName: String,
        size: Int,
        offset: Int,
        sinceTimeStamp: Long,
        filter: String?,
    ): FintResources {
        val cache = timed(resourceName, "cache.getCache") { cacheService.getCache(resourceName) }
        val resources = timed(resourceName, "cache.getList") { cache.getList(size.toLong(), offset.toLong(), sinceTimeStamp, filter) }

        return timed(resourceName, "links.toResources") {
            linkService.toResources(resourceName, resources, offset, size, cache.size)
        }
    }

    fun getResourceById(
        resourceName: String,
        idField: String,
        idValue: String,
    ): FintResource? {
        val cache = timed(resourceName, "cache.getCache") { cacheService.getCache(resourceName) }
        return timed(resourceName, "cache.getByIdField") { cache.getByIdField(idField, idValue) }
    }

    fun getLastUpdated(resourceName: String): Long {
        val cache = timed(resourceName, "cache.getCache") { cacheService.getCache(resourceName) }
        return timed(resourceName, "cache.getLastUpdated") { cache.lastUpdated }
    }

    fun getCacheSize(resourceName: String): Int {
        val cache = timed(resourceName, "cache.getCache") { cacheService.getCache(resourceName) }
        return timed(resourceName, "cache.getSize") { cache.size }
    }

    private fun <T> timed(
        resourceName: String,
        operation: String,
        supplier: () -> T,
    ): T {
        val sample = Timer.start(meterRegistry)
        var status = "success"

        try {
            return supplier()
        } catch (runtimeException: RuntimeException) {
            status = "error"
            throw runtimeException
        } finally {
            val duration = Duration.ofNanos(sample.stop(timer(resourceName, operation, status)))
            if (duration > SLOW_COMPONENT_THRESHOLD) {
                logger.warn(
                    "Slow processing component detected: operation={}, durationMs={}, resource={}, org={}, status={}",
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
            .builder("core.consumer.processing")
            .description("Duration of internal processing steps for resource operations")
            .tag("org", consumerConfiguration.orgId)
            .tag("resource", safeResourceName(resourceName))
            .tag("operation", operation)
            .tag("status", status)
            .register(meterRegistry)

    private fun safeResourceName(resourceName: String?): String =
        if (resourceName.isNullOrBlank()) "unknown" else resourceName

    companion object {
        private val logger = LoggerFactory.getLogger(ResourceService::class.java)
        private val SLOW_COMPONENT_THRESHOLD: Duration = Duration.ofSeconds(10)
    }
}
