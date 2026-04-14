package no.fintlabs.consumer.resource

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.sync.LastCompletedFullSync
import no.fintlabs.consumer.resource.context.ResourceContext
import org.springframework.stereotype.Component

@Component
class ResourceMetrics(
    private val meterRegistry: MeterRegistry,
    private val cacheService: CacheService,
    private val resourceContext: ResourceContext,
    private val configuration: ConsumerConfiguration,
    private val syncCache: LastCompletedFullSync
) {
    @PostConstruct
    private fun init() {
        resourceContext.resources.forEach { resource -> registerCacheSize(resource.name()) }
        resourceContext.resources.forEach { resource -> registerLatestFullSync(resource.name()) }
    }

    private fun registerCacheSize(resourceName: String) {
        Gauge
            .builder("core.cache.size") { cacheService.getCache(resourceName).size }
            .tag("resource", resourceName)
            .tag("org", configuration.orgId.value)
            .description("Number of entries in the cache for a given resource")
            .register(meterRegistry)
    }

    private fun registerLatestFullSync(resourceName: String) {
        Gauge
            .builder("core.consumer.latestCompletedFullSync") {syncCache.getLatestFromResource(resourceName)}
            .tag("resource", resourceName)
            .tag("org", configuration.orgId.value)
            .description("Timestamp of latest completed FullSync for $resourceName")
            .register(meterRegistry)
    }
}
