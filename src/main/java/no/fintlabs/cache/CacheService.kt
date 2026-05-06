package no.fintlabs.cache

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PreDestroy
import no.fintlabs.consumer.config.FintCacheProperties
import no.novari.fint.model.resource.FintResource
import org.springframework.stereotype.Service
import java.io.File
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Service
class CacheService(
    private val properties: FintCacheProperties,
    private val objectMapper: ObjectMapper,
) {
    private val basePath: String
    private val resourceCaches: MutableMap<String, FintCache<FintResource>> = ConcurrentHashMap()

    init {
        val root = File(properties.basePath.trimEnd('/'))
        root.mkdirs()
        // Wipe all subdirectories from previous runs. Cache is always rebuilt from Kafka,
        // so any existing content is stale. This also cleans up orphans from killed processes.
        root.listFiles()?.forEach { it.deleteRecursively() }
        basePath = "${root.path}/${UUID.randomUUID().toString().take(8)}"
    }

    fun getCachedResourceNames(): Set<String> = resourceCaches.keys

    fun getCache(resourceName: String): FintCache<FintResource> =
        resourceCaches.computeIfAbsent(resourceName.lowercase()) {
            FintCache(
                dbPath = "$basePath/$it",
                blockCacheSizeBytes = properties.blockCacheSizeMb * 1024L * 1024L,
                objectMapper = objectMapper,
            )
        }

    @PreDestroy
    fun close() {
        resourceCaches.values.forEach { it.close() }
        File(basePath).deleteRecursively()
    }
}
