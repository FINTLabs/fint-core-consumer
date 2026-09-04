package no.fintlabs.consumer.resource

import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.model.resource.FintResources
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service

@Service
class ResourceService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
) {
    fun getResources(
        resourceName: String,
        size: Int,
        offset: Int,
        sinceTimeStamp: Long,
        filter: String?,
    ): FintResources {
        val cache = cacheService.getCache(resourceName)
        val page = cache.getPage(size.toLong(), offset.toLong(), sinceTimeStamp, filter)
        return linkService.toResources(
            resourceName,
            page.resources,
            offset,
            size,
            page.totalItems,
            sinceTimeStamp,
            filter,
        )
    }

    fun getResourceById(
        resourceName: String,
        idField: String,
        idValue: String,
    ): FintResource? = cacheService.getCache(resourceName).getByIdField(idField, idValue)

    fun getLastUpdated(resourceName: String): Long = cacheService.getCache(resourceName).lastUpdated

    fun getCacheSize(resourceName: String): Int = cacheService.getCache(resourceName).size
}
