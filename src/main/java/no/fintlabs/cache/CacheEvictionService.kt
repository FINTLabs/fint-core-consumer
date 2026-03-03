package no.fintlabs.cache

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import no.fintlabs.autorelation.RelationEventService
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val relationEventService: RelationEventService,
) {
    suspend fun evictExpired(
        resourceName: String,
        startTimestamp: Long,
    ) = coroutineScope {
        cacheService
            .getCache(resourceName)
            .evictExpired(startTimestamp)
            .map { (resourceId, resource) ->
                async { publishRelationDeleteRequest(resourceName, resourceId, resource) }
            }.awaitAll()
    }

    private fun publishRelationDeleteRequest(
        resourceName: String,
        resourceId: String,
        resource: FintResource,
    ) = relationEventService.removeRelations(resourceName, resourceId, resource)
}
