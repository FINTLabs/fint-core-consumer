package no.fintlabs.cache

import no.fint.antlr.odata.ODataFilterService
import no.fint.model.resource.FintResource
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Stream
import kotlin.concurrent.read
import kotlin.concurrent.write

class FintCache<T : FintResource> {
    private val indexMap: MutableMap<String, ConcurrentHashMap<String, CacheEntry>> =
        mutableMapOf<String, ConcurrentHashMap<String, CacheEntry>>()
    private val entryStore: LinkedHashMap<String, CacheEntry> = LinkedHashMap<String, CacheEntry>()
    private val lock = ReentrantReadWriteLock()

    inner class CacheEntry(
        val resource: T,
        val timestamp: Long
    )

    fun put(resourceId: String, resource: T, timestamp: Long) {
        val entry = CacheEntry(resource, timestamp)

        lock.write {
            val existing = entryStore[resourceId]
            if (existing != null) {
                removeFromIndexes(existing.resource)
                entryStore[resourceId] = entry  // preserves order
            } else {
                entryStore[resourceId] = entry  // insert at end
            }
            updateIndexes(entry)
        }
    }

    fun get(resourceId: String): T? = lock.read {
        entryStore[resourceId]?.resource
    }

    fun getByIdField(field: String, value: Any): T? = lock.read {
        indexMap[field.lowercase()]?.get(value)?.resource
    }

    fun getStream(size: Long, offset: Long, sinceTimestamp: Long, filter: String?): Stream<T> = lock.read {
        var entries = entryStore.values.stream()
        if (sinceTimestamp > 0L) {
            // Only include entries at or after provided timestamp
            entries = entries.filter { entry -> entry.timestamp >= sinceTimestamp }
        }

        var resources = entries.map { it.resource }
        if (filter != null && !filter.isBlank()) {
            // Only include entries matching OData $filter
            resources = applyODataFilter(resources, filter)
        }

        if (size > 0) {
            // Only include entries for requested page
            if (offset > 0) {
                resources = resources.skip(offset)
            }
            resources = resources.limit(size)
        }

        resources
    }

    private fun applyODataFilter(
        resources: Stream<T>,
        filter: String
    ): Stream<T> {
        val oDataFilterService = ODataFilterService()
        if (!oDataFilterService.validate(filter)) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid OData filter")
        }

        return oDataFilterService.from(resources, filter)
    }

    fun getLastUpdated(): Long = lock.read {
        entryStore.values.maxOfOrNull { it.timestamp } ?: -1L
    }

    fun size(): Int = lock.read { entryStore.size }

    fun remove(requiredId: Any) = lock.write {
        val entry = entryStore.remove(requiredId)
        if (entry != null) {
            removeFromIndexes(entry.resource)
        }
    }

    /**
     * Evict expired cache entries. A cached entry is considered expired if it has a timestamp
     * older than the earliest timestamp of a full-sync.
     *
     * @param timestamp earliest timestamp of a full-sync.
     * @return evicted resources
     */
    fun evictExpired(timestamp: Long): Set<T> = lock.write {
        val removedResources = mutableSetOf<T>()

        val iterator = entryStore.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.value.timestamp < timestamp) {
                removedResources.add(entry.value.resource)
                iterator.remove()
            }
        }

        return removedResources
    }

    private fun updateIndexes(entry: CacheEntry) {
        entry.resource.identifikators
            .filter { entry -> entry.value != null }
            .forEach { (key, value) ->
                indexMap.computeIfAbsent(key.lowercase()) { ConcurrentHashMap() }[value.identifikatorverdi] = entry
            }
    }

    private fun removeFromIndexes(resource: T) {
        resource.identifikators.forEach { (key, value) ->
            indexMap[key.lowercase()]?.remove(value.identifikatorverdi)
        }
    }
}