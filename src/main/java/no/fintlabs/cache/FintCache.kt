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

/**
 * Thread-safe in-memory cache for [FintResource] instances.
 *
 * The cache stores entries by resource ID in insertion order and maintains a secondary index
 * by identifier key/value for fast `getByIdField` lookups. Each cached resource is associated
 * with a timestamp used for incremental reads (`sinceTimestamp`), expiration (`evictExpired`),
 * and last-update tracking.
 */
class FintCache<T : FintResource> {
    private val index: MutableMap<IndexKey, CacheEntry> = mutableMapOf()
    private val entryStore: LinkedHashMap<String, CacheEntry> = LinkedHashMap<String, CacheEntry>()
    private val lastUpdatedTimestamp = AtomicLong(0L)
    private val lock = ReentrantReadWriteLock()
    private val oDataFilterService = ODataFilterService()

    /**
     * Internal cache value containing the resource and its write timestamp.
     */
    inner class CacheEntry(
        /** Cached resource instance. */
        val resource: T,
        /** Timestamp used for change tracking, filtering, and eviction. */
        val timestamp: Long,
    )

    /**
     * Composite key for [index], based on identifier key and identifier value.
     *
     * The identifier key is normalized to lowercase to make lookups case-insensitive.
     */
    private class IndexKey(
        idKey: String,
        val idValue: Any,
    ) {
        val idKey: String = idKey.lowercase()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is IndexKey) return false
            return idKey == other.idKey && idValue == other.idValue
        }

        override fun hashCode(): Int = 31 * idKey.hashCode() + idValue.hashCode()
    }

    /**
     * Insert or replace a resource in the cache.
     *
     * Updates the identifier index and advances [lastUpdated] with the provided timestamp.
     */
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

    /**
     * Get a cached resource by resource ID.
     *
     * @return the cached resource, or `null` if not present.
     */
    fun get(resourceId: String): T? =
        lock.read {
            entryStore[resourceId]?.resource
        }

    /**
     * Get the write timestamp for a cached resource.
     *
     * @return timestamp of the cached resource, or `null` if not present.
     */
    fun lastUpdatedByResourceId(resourceId: String): Long? =
        lock.read {
            entryStore[resourceId]?.timestamp
        }

    /**
     * Get a cached resource by identifier field and value.
     *
     * Identifier field matching is case-insensitive.
     */
    fun getByIdField(
        field: String,
        value: Any,
    ): T? =
        lock.read {
            index[IndexKey(field, value)]?.resource
        }

    /**
     * Get a paged list of cached resources, optionally filtered by timestamp and OData filter.
     *
     * When [sinceTimestamp] is greater than `0`, only resources updated at or after that
     * timestamp are included. When [size] is greater than `0`, pagination is applied using
     * [offset] and [size].
     */
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
        if (!oDataFilterService.validate(filter)) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid OData filter")
        }

        return oDataFilterService.from(resources, filter)
    }

    /**
     * Highest timestamp seen by the cache from write/remove operations.
     */
    var lastUpdated: Long
        get() =
            lock.read {
                return lastUpdatedTimestamp.get()
            }
        private set(value) =
            lock.write {
                lastUpdatedTimestamp.accumulateAndGet(value) { existing, new -> max(existing, new) }
            }

    /**
     * Current number of cached resources.
     */
    val size: Int
        get() = lock.read { entryStore.size }

    /**
     * Remove a resource by ID.
     *
     * If the resource exists, its index entries are removed and [lastUpdated] is advanced
     * with the provided timestamp.
     */
    fun remove(
        resourceId: String,
        timestamp: Long,
    ) = lock.write {
        val entry = entryStore.remove(resourceId)
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
                index[IndexKey(key, value.identifikatorverdi)] = entry
            }
    }

    private fun removeFromIndexes(resource: T) {
        resource.identifikators
            .filter { entry -> entry.value?.identifikatorverdi != null }
            .forEach { (key, value) ->
                index.remove(IndexKey(key, value.identifikatorverdi))
            }
    }
}
