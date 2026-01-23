package no.fintlabs.cache

import no.fintlabs.autorelation.RelationEventService
import org.springframework.stereotype.Service

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val relationEventService: RelationEventService,
) {
    fun evictExpired(resourceName: String, startTimestamp: Long) =
        cacheService.getCache(resourceName)
            .evictExpired(startTimestamp)
            .forEach { publishRelationDeleteRequest(resourceName, it) }

    private fun publishRelationDeleteRequest(
        resourceName: String,
        resource: Any,
    ) = relationEventService.removeRelations(resourceName, TODO, resource.unboxObject())
}
