package no.fintlabs.cache

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PreDestroy
import no.fintlabs.consumer.config.FintCacheProperties
import no.novari.fint.model.resource.FintResource
import org.rocksdb.LRUCache
import org.rocksdb.RocksDB
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
    private val sharedBlockCache: LRUCache
    private val resourceCaches: MutableMap<String, FintCache<FintResource>> = ConcurrentHashMap()

    init {
        RocksDB.loadLibrary()
        val root = File(properties.basePath.trimEnd('/'))
        root.mkdirs()
        // Wipe all subdirectories from previous runs. Cache is always rebuilt from Kafka,
        // so any existing content is stale. This also cleans up orphans from killed processes.
        root.listFiles()?.forEach { it.deleteRecursively() }
        basePath = "${root.path}/${UUID.randomUUID().toString().take(8)}"
        sharedBlockCache = LRUCache(properties.blockCacheSizeMb * 1024L * 1024L)
    }

    fun getCachedResourceNames(): Set<String> = resourceCaches.keys

    fun getCache(resourceName: String): FintCache<FintResource> =
        resourceCaches.computeIfAbsent(resourceName.lowercase()) {
            FintCache(
                dbPath = "$basePath/$it",
                sharedBlockCache = sharedBlockCache,
                writeBufferSizeBytes = properties.writeBufferMb * 1024L * 1024L,
                totalWriteBufferSizeBytes = properties.totalWriteBufferMb * 1024L * 1024L,
                objectMapper = objectMapper,
            )
        }

    @PreDestroy
    fun close() {
        resourceCaches.values.forEach { it.close() }
        File(basePath).deleteRecursively()
        sharedBlockCache.close()
    }
}
