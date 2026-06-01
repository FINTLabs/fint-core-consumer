package no.fintlabs.cache

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.Sorts
import no.fint.antlr.odata.ODataFilterService
import no.fintlabs.cache.CacheDocumentCodec.Companion.FIELD_ID
import no.fintlabs.cache.CacheDocumentCodec.Companion.FIELD_IDENTIFIERS
import no.fintlabs.cache.CacheDocumentCodec.Companion.FIELD_IDENTIFIER_KEY
import no.fintlabs.cache.CacheDocumentCodec.Companion.FIELD_IDENTIFIER_VALUE
import no.fintlabs.cache.CacheDocumentCodec.Companion.FIELD_TIMESTAMP
import no.novari.fint.model.resource.FintResource
import org.bson.Document
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.util.Spliterator
import java.util.Spliterators
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Stream
import java.util.stream.StreamSupport
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.max

/**
 * Mongo-backed cache for [FintResource] instances.
 *
 * Each instance owns a single Mongo collection holding documents produced by [CacheDocumentCodec].
 * The collection is sorted by `(timestamp, _id)` for stable pagination and secondary-indexed by
 * identifier key/value for fast [getByIdField] lookups.
 *
 * A per-instance [ReentrantReadWriteLock] serialises writes within the JVM so the
 * timestamp-monotonicity guarantee (`put` rejects older timestamps) holds without relying on
 * Mongo-level conditional writes.
 */
class MongoDBFintCache(
    private val mongoTemplate: MongoTemplate,
    private val codec: CacheDocumentCodec,
    private val collectionName: String,
) : FintCache {
    private val lastUpdatedTimestamp = AtomicLong(0L)
    private val lock = ReentrantReadWriteLock()
    private val oDataFilterService = ODataFilterService()

    init {
        ensureIndexes()
        primeLastUpdated()
    }

    private fun collection(): MongoCollection<Document> = mongoTemplate.getCollection(collectionName)

    private fun ensureIndexes() {
        val coll = collection()
        coll.createIndex(
            Indexes.compoundIndex(Indexes.ascending(FIELD_TIMESTAMP), Indexes.ascending(FIELD_ID)),
            IndexOptions().name("timestamp_id_idx"),
        )
        coll.createIndex(
            Indexes.compoundIndex(
                Indexes.ascending("$FIELD_IDENTIFIERS.$FIELD_IDENTIFIER_KEY"),
                Indexes.ascending("$FIELD_IDENTIFIERS.$FIELD_IDENTIFIER_VALUE"),
            ),
            IndexOptions().name("identifiers_idx"),
        )
    }

    private fun primeLastUpdated() {
        val top =
            collection()
                .find()
                .projection(Document(FIELD_TIMESTAMP, 1))
                .sort(Sorts.descending(FIELD_TIMESTAMP))
                .limit(1)
                .first()
        if (top != null) {
            lastUpdatedTimestamp.set(top.getLong(FIELD_TIMESTAMP))
        }
    }

    /**
     * Insert or replace a resource in the cache.
     *
     * @return `true` if the write was accepted, `false` if it was rejected because an existing
     *   entry has a newer timestamp.
     */
    override fun put(
        resourceId: String,
        resource: FintResource,
        timestamp: Long,
    ): Boolean =
        lock.write {
            val existingTs = lookupTimestamp(resourceId)
            if (existingTs != null && timestamp < existingTs) return@write false

            val doc = codec.toDocument(resourceId, resource, timestamp)
            collection().replaceOne(
                Document(FIELD_ID, resourceId),
                doc,
                ReplaceOptions().upsert(true),
            )
            lastUpdated = timestamp
            true
        }

    override fun get(resourceId: String): FintResource? =
        lock.read {
            val doc = collection().find(Document(FIELD_ID, resourceId)).first() ?: return@read null
            codec.fromDocument(doc)
        }

    override fun lastUpdatedByResourceId(resourceId: String): Long? =
        lock.read {
            lookupTimestamp(resourceId)
        }

    private fun lookupTimestamp(resourceId: String): Long? =
        collection()
            .find(Document(FIELD_ID, resourceId))
            .projection(Document(FIELD_TIMESTAMP, 1))
            .first()
            ?.getLong(FIELD_TIMESTAMP)

    override fun getByIdField(
        field: String,
        value: Any,
    ): FintResource? =
        lock.read {
            val criteria =
                Document(
                    FIELD_IDENTIFIERS,
                    Document(
                        "\$elemMatch",
                        Document(FIELD_IDENTIFIER_KEY, field.lowercase())
                            .append(FIELD_IDENTIFIER_VALUE, value.toString()),
                    ),
                )
            val doc = collection().find(criteria).first() ?: return@read null
            codec.fromDocument(doc)
        }

    /**
     * Get a paged, `(timestamp, _id)`-sorted list of cached resources, optionally filtered.
     *
     * When [filter] is supplied the cursor is streamed and filtering is applied in-app via
     * [ODataFilterService] before pagination so OData semantics remain unchanged.
     */
    override fun getList(
        size: Long,
        offset: Long,
        sinceTimestamp: Long,
        filter: String?,
    ): List<FintResource> =
        lock.read {
            val criteria =
                if (sinceTimestamp > 0L) {
                    Document(FIELD_TIMESTAMP, Document("\$gte", sinceTimestamp))
                } else {
                    Document()
                }
            val cursor =
                collection()
                    .find(criteria)
                    .sort(Sorts.ascending(FIELD_TIMESTAMP, FIELD_ID))
                    .iterator()

            cursor.use { c ->
                val baseStream =
                    StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(c, Spliterator.ORDERED or Spliterator.NONNULL),
                        false,
                    )

                var resources: Stream<FintResource> = baseStream.map { codec.fromDocument(it) }
                if (!filter.isNullOrBlank()) {
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
        }

    private fun applyODataFilter(
        resources: Stream<FintResource>,
        filter: String,
    ): Stream<FintResource> {
        if (!oDataFilterService.validate(filter)) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid OData filter")
        }
        return oDataFilterService.from(resources, filter)
    }

    override var lastUpdated: Long
        get() = lastUpdatedTimestamp.get()
        private set(value) {
            lastUpdatedTimestamp.accumulateAndGet(value) { existing, new -> max(existing, new) }
        }

    override val size: Int
        get() = lock.read { collection().countDocuments().toInt() }

    override fun remove(
        resourceId: String,
        timestamp: Long,
    ) = lock.write {
        val result =
            collection().deleteOne(
                Document(FIELD_ID, resourceId)
                    .append(FIELD_TIMESTAMP, Document("\$lt", timestamp)),
            )
        if (result.deletedCount > 0) {
            lastUpdated = timestamp
        }
    }

    /**
     * Evict cache entries with `timestamp < [timestamp]`. Returns the evicted `(id, resource)`
     * pairs so callers can publish relation deletes for them.
     *
     * The entire expired set is materialised in heap; callers must accept that footprint. For the
     * typical full-sync sweep this is bounded by the number of stale entries for a single
     * resource type.
     */
    override fun evictExpired(timestamp: Long): Set<Pair<String, FintResource>> =
        lock.write {
            val criteria = Document(FIELD_TIMESTAMP, Document("\$lt", timestamp))
            val coll = collection()
            val expired = mutableSetOf<Pair<String, FintResource>>()
            coll.find(criteria).iterator().use { cursor ->
                while (cursor.hasNext()) {
                    val doc = cursor.next()
                    expired.add(codec.resourceId(doc) to (codec.fromDocument(doc)))
                }
            }
            if (expired.isNotEmpty()) {
                coll.deleteMany(criteria)
            }
            expired
        }
}
