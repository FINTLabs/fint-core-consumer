package no.fintlabs.cache

import no.novari.fint.model.resource.FintResource
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

@Service
class CacheService(
    private val mongoTemplate: MongoTemplate,
    private val codec: CacheDocumentCodec,
) {
    private val resourceCaches: MutableMap<String, FintCache<FintResource>> =
        ConcurrentHashMap<String, FintCache<FintResource>>()

    fun getCachedResourceNames(): Set<String> = resourceCaches.keys

    fun getCache(resourceName: String): FintCache<FintResource> {
        val key = resourceName.lowercase()
        return resourceCaches.computeIfAbsent(key) {
            FintCache(mongoTemplate, codec, "$COLLECTION_PREFIX$key")
        }
    }

    companion object {
        const val COLLECTION_PREFIX = "cache_"
    }
}
