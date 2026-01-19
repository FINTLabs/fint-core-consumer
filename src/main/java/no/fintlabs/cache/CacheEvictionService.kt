package no.fintlabs.cache

import no.fintlabs.consumer.config.ConsumerConfiguration
import org.springframework.stereotype.Service

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
) {
    fun evictExpired(resourceName: String) =
        cacheService
            .getCache(resourceName)
            ?.let { cache ->
                cache.evictOldCacheObjects { key, cacheObject ->
                    onCacheEviction(key, resourceName, cacheObject.unboxObject())
                }
            }

    private fun onCacheEviction(
        key: String,
        resourceName: String,
        resource: Any,
    ) {
        // TODO: If it has managed relations, send relation update
    }
}
