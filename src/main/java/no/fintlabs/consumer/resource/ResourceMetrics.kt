package no.fintlabs.consumer.resource

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.resource.context.ResourceContext
import org.springframework.stereotype.Component

@Component
class ResourceMetrics(
    private val meterRegistry: MeterRegistry,
    private val cacheService: CacheService,
    private val resourceContext: ResourceContext,
    private val configuration: ConsumerConfiguration,
) {
    @PostConstruct
    private fun init() {
        resourceContext.resources.forEach { resource -> registerCacheSize(resource.name()) }
    }

    private fun registerCacheSize(resourceName: String) {
        Gauge
            .builder("core.cache.size") { cacheService.getCache(resourceName).size }
            .tag("resource", resourceName)
            .tag("org", configuration.orgId.value)
            .description("Number of entries in the cache for a given resource")
            .register(meterRegistry)
    }
}
