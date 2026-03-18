package no.fintlabs.cache

import no.fint.antlr.odata.ODataFilterService
import no.novari.fint.model.resource.FintResource
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.util.TreeMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Stream
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.max

/**
 * Thread-safe in-memory cache for [FintResource] instances.
 *
 * The cache stores entries by resource ID and maintains two indexes:
 * - [entryStore]: `HashMap` for O(1) lookup by resource ID
 * - [sortedIndex]: `TreeMap` keyed by timestamp for O(log n) ordered iteration and range queries
 *
 * Entries are always iterated in ascending publish-timestamp order. Concurrent writes from
 * multiple Kafka consumer threads are therefore naturally ordered regardless of the thread
 * scheduling that delivered them. When two records carry the same timestamp the first one
 * to arrive is kept ([put] is a no-op for equal or older timestamps).
 *
 * Each cached resource is associated with a timestamp used for incremental reads
 * (`sinceTimestamp`), expiration (`evictExpired`), and last-update tracking.
 */
class FintCache<T : FintResource> {
    private val index: MutableMap<IndexKey, CacheEntry> = mutableMapOf()
    private val entryStore: HashMap<String, CacheEntry> = HashMap()
    private val sortedIndex: TreeMap<Long, MutableSet<String>> = TreeMap()
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
     * The entry is only written if [timestamp] is strictly greater than the timestamp of the
     * existing entry for [resourceId]. Records with the same or older timestamp are ignored to
     * preserve the publish-time ordering guarantee enforced by [sortedIndex].
     *
     * Updates the identifier index and advances [lastUpdated] with the provided timestamp.
     */
    fun put(
        resourceId: String,
        resource: T,
        timestamp: Long,
    ) {
        lock.write {
            val existing = entryStore[resourceId]
            if (existing != null && existing.timestamp >= timestamp) return

            if (existing != null) {
                removeFromIndexes(existing.resource)
                removeSortedEntry(resourceId, existing.timestamp)
            }

            val entry = CacheEntry(resource, timestamp)
            entryStore[resourceId] = entry
            sortedIndex.getOrPut(timestamp) { mutableSetOf() }.add(resourceId)
            updateIndexes(entry)
            lastUpdated = timestamp
        }
    }

    /**
     * Update the resource of an existing cache entry without changing its timestamp or position
     * in [sortedIndex].
     *
     * Intended for in-place mutations such as applying relation link changes to a resource that
     * is already cached. The timestamp guard in [put] does not apply here — the caller has
     * already verified that the entry exists and is applying an incremental change on top of it.
     *
     * If [resourceId] is not present in the cache this is a no-op.
     */
    fun forceUpdate(
        resourceId: String,
        resource: T,
    ) {
        lock.write {
            val existing = entryStore[resourceId] ?: return
            removeFromIndexes(existing.resource)
            val entry = CacheEntry(resource, existing.timestamp)
            entryStore[resourceId] = entry
            updateIndexes(entry)
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
     * Get a paged list of cached resources in ascending publish-timestamp order, optionally
     * filtered by timestamp and OData filter.
     *
     * When [sinceTimestamp] is greater than `0`, only resources updated at or after that
     * timestamp are included — resolved via [sortedIndex] in O(log n). When [size] is
     * greater than `0`, pagination is applied using [offset] and [size].
     */
    fun getList(
        size: Long,
        offset: Long,
        sinceTimestamp: Long,
        filter: String?,
    ): List<T> =
        lock.read {
            val buckets = if (sinceTimestamp > 0L) sortedIndex.tailMap(sinceTimestamp) else sortedIndex

            var resources: Stream<T> =
                buckets.values
                    .stream()
                    .flatMap { it.stream() }
                    .map { entryStore[it]!!.resource }

            if (filter != null && !filter.isBlank()) {
                resources = applyODataFilter(resources, filter)
            }

            if (size > 0) {
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
     * The entry is only removed if [timestamp] is greater than or equal to the timestamp of
     * the existing entry (latest delete wins). Stale delete records with an older timestamp
     * are ignored.
     *
     * If the resource exists and the timestamp guard passes, its index entries are removed and
     * [lastUpdated] is advanced with the provided timestamp.
     */
    fun remove(
        resourceId: String,
        timestamp: Long,
    ) {
        lock.write {
            val entry = entryStore[resourceId]
            if (entry == null || entry.timestamp > timestamp) return@write

            entryStore.remove(resourceId)
            removeFromIndexes(entry.resource)
            removeSortedEntry(resourceId, entry.timestamp)
            lastUpdated = timestamp
        }
    }

    /**
     * Evict expired cache entries. A cached entry is considered expired if it has a timestamp
     * older than the earliest timestamp of a full-sync.
     *
     * Uses [sortedIndex] to efficiently locate all entries with timestamp strictly less than
     * [timestamp] without scanning the full store.
     *
     * @param timestamp earliest timestamp of a full-sync.
     * @return evicted resources
     */
    fun evictExpired(timestamp: Long): Set<Pair<String, T>> =
        lock.write {
            val removedResources = mutableSetOf<Pair<String, T>>()

            val expiredBuckets = sortedIndex.headMap(timestamp)
            expiredBuckets.values.forEach { resourceIds ->
                resourceIds.forEach { resourceId ->
                    val entry = entryStore.remove(resourceId)
                    if (entry != null) {
                        removedResources.add(Pair(resourceId, entry.resource))
                        removeFromIndexes(entry.resource)
                    }
                }
            }
            expiredBuckets.clear()

            removedResources
        }

    private fun removeSortedEntry(
        resourceId: String,
        timestamp: Long,
    ) {
        val bucket = sortedIndex[timestamp] ?: return
        bucket.remove(resourceId)
        if (bucket.isEmpty()) sortedIndex.remove(timestamp)
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
