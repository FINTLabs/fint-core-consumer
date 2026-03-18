package no.fintlabs.cache

import no.fintlabs.consumer.resource.context.ResourceContext
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

@Service
class CacheService(
    resourceContext: ResourceContext,
) {
    /** Eagerly initializes a [FintCache] for each resource name configured in [ResourceContext]. */
    private val resourceCaches: MutableMap<String, FintCache<FintResource>> =
        resourceContext.resourceNames.associateWithTo(ConcurrentHashMap()) { FintCache() }

    fun getCachedResourceNames(): Set<String> = resourceCaches.keys

    fun getCache(resourceName: String): FintCache<FintResource> =
        resourceCaches.computeIfAbsent(resourceName.lowercase()) { FintCache() }
}
