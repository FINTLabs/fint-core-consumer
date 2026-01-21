package no.fintlabs.cache

import no.fintlabs.autorelation.RelationEventService
import org.springframework.stereotype.Service

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val relationEventService: RelationEventService,
) {
    fun evictExpired(resourceName: String) =
        cacheService
            .getCache(resourceName)
            ?.let { cache ->
                cache.evictOldCacheObjects { key, cacheObject ->
                    relationEventService.removeRelations(resourceName, key, cacheObject.unboxObject())
                }
            }
}
