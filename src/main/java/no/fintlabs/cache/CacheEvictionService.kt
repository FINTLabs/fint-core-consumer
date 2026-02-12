package no.fintlabs.cache

import no.fintlabs.autorelation.RelationEventService
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val relationEventService: RelationEventService,
) {
    fun evictExpired(
        resourceName: String,
        startTimestamp: Long,
    ) = cacheService
        .getCache(resourceName)
        .evictExpired(startTimestamp)
        .forEach { publishRelationDeleteRequest(resourceName, it.first, it.second) }

    private fun publishRelationDeleteRequest(
        resourceName: String,
        resourceId: String,
        resource: FintResource,
    ) = relationEventService.removeRelations(resourceName, resourceId, resource)
}
