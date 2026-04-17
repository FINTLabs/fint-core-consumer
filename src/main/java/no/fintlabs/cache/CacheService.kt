package no.fintlabs.cache

import io.micrometer.core.instrument.MeterRegistry
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

@Service
class CacheService(
    private val meterRegistry: MeterRegistry,
) {
    private val resourceCaches: MutableMap<String, FintCache<FintResource>> =
        ConcurrentHashMap<String, FintCache<FintResource>>()

    fun getCachedResourceNames(): Set<String> = resourceCaches.keys

    fun getCache(resourceName: String): FintCache<FintResource> =
        resourceCaches.computeIfAbsent(resourceName.lowercase()) { FintCache(it, meterRegistry) }
}
