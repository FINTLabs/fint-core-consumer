package no.fintlabs.cache

import no.fint.antlr.odata.ODataFilterService
import no.novari.fint.model.resource.FintResource
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Stream
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.max

class FintCache<T : FintResource> {
    private val indexMap: MutableMap<String, HashMap<String, CacheEntry>> =
        mutableMapOf<String, HashMap<String, CacheEntry>>()
    private val entryStore: LinkedHashMap<String, CacheEntry> = LinkedHashMap<String, CacheEntry>()
    private val lastUpdatedTimestamp = AtomicLong(0L)
    private val lock = ReentrantReadWriteLock()

    inner class CacheEntry(
        val resource: T,
        val timestamp: Long,
    )

    fun put(
        resourceId: String,
        resource: T,
        timestamp: Long,
    ) {
        val entry = CacheEntry(resource, timestamp)

        lock.write {
            val existing = entryStore[resourceId]
            if (existing != null) {
                removeFromIndexes(existing.resource)
                entryStore[resourceId] = entry // preserves order
            } else {
                entryStore[resourceId] = entry // insert at end
            }
            updateIndexes(entry)
            lastUpdated = timestamp
        }
    }

    fun get(resourceId: String): T? =
        lock.read {
            entryStore[resourceId]?.resource
        }

    fun lastUpdatedByResourceId(resourceId: String): Long? =
        lock.read {
            entryStore[resourceId]?.timestamp
        }

    fun getByIdField(
        field: String,
        value: Any,
    ): T? =
        lock.read {
            indexMap[field.lowercase()]?.get(value)?.resource
        }

    fun getList(
        size: Long,
        offset: Long,
        sinceTimestamp: Long,
        filter: String?,
    ): List<T> =
        lock.read {
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

            resources.toList()
        }

    private fun applyODataFilter(
        resources: Stream<T>,
        filter: String,
    ): Stream<T> {
        val oDataFilterService = ODataFilterService()
        if (!oDataFilterService.validate(filter)) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid OData filter")
        }

        return oDataFilterService.from(resources, filter)
    }

    var lastUpdated: Long
        get() =
            lock.read {
                return lastUpdatedTimestamp.get()
            }
        private set(value) =
            lock.write {
                lastUpdatedTimestamp.accumulateAndGet(value) { existing, new -> max(existing, new) }
            }

    val size: Int
        get() = lock.read { entryStore.size }

    fun remove(
        requiredId: String,
        timestamp: Long,
    ) = lock.write {
        val entry = entryStore.remove(requiredId)
        if (entry != null) {
            removeFromIndexes(entry.resource)
            lastUpdated = timestamp
        }
    }

    /**
     * Evict expired cache entries. A cached entry is considered expired if it has a timestamp
     * older than the earliest timestamp of a full-sync.
     *
     * @param timestamp earliest timestamp of a full-sync.
     * @return evicted resources
     */
    fun evictExpired(timestamp: Long): Set<Pair<String, T>> =
        lock.write {
            val removedResources = mutableSetOf<Pair<String, T>>()

            val iterator = entryStore.iterator()
            while (iterator.hasNext()) {
                val entry = iterator.next()
                if (entry.value.timestamp < timestamp) {
                    removedResources.add(Pair(entry.key, entry.value.resource))
                    removeFromIndexes(entry.value.resource)
                    iterator.remove()
                }
            }

            return removedResources
        }

    private fun updateIndexes(entry: CacheEntry) {
        entry.resource.identifikators
            .filter { entry -> entry.value?.identifikatorverdi != null }
            .forEach { (key, value) ->
                indexMap.computeIfAbsent(key.lowercase()) { HashMap() }[value.identifikatorverdi] = entry
            }
    }

    private fun removeFromIndexes(resource: T) {
        resource.identifikators
            .filter { entry -> entry.value?.identifikatorverdi != null }
            .forEach { (key, value) ->
                indexMap[key.lowercase()]?.remove(value.identifikatorverdi)
            }
    }
}
