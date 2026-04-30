package no.fintlabs.cache

import com.fasterxml.jackson.databind.ObjectMapper
import no.fint.antlr.odata.ODataFilterService
import no.novari.fint.model.resource.FintResource
import org.rocksdb.BlockBasedTableConfig
import org.rocksdb.ColumnFamilyDescriptor
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.ColumnFamilyOptions
import org.rocksdb.DBOptions
import org.rocksdb.LRUCache
import org.rocksdb.RocksDB
import org.rocksdb.RocksIterator
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.io.Closeable
import java.io.File
import java.nio.ByteBuffer
import java.util.Spliterator
import java.util.Spliterators
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer
import java.util.stream.Stream
import java.util.stream.StreamSupport
import kotlin.concurrent.withLock
import kotlin.math.max

/**
 * Disk-backed cache for [FintResource] instances, using RocksDB.
 *
 * Three column families:
 *  - default (entries): resourceId → [8B timestamp][4B classNameLen][className][json]
 *  - sorted:            [8B timestamp (big-endian)][resourceId] → resourceId
 *  - index:             [idKey\0idValue] → resourceId
 *
 * Data is destroyed and rebuilt from Kafka on every restart, so the DB directory is
 * wiped on construction. Callers must [close] the instance on shutdown.
 */
class FintCache<T : FintResource>(
    dbPath: String,
    sharedBlockCache: LRUCache,
    writeBufferSizeBytes: Long,
    totalWriteBufferSizeBytes: Long,
    private val objectMapper: ObjectMapper,
) : Closeable {
    companion object {
        init {
            RocksDB.loadLibrary()
        }
    }

    private val db: RocksDB
    private val entriesCf: ColumnFamilyHandle
    private val sortedCf: ColumnFamilyHandle
    private val indexCf: ColumnFamilyHandle
    private val writeOptions = WriteOptions()
    private val writeLock = ReentrantLock()
    private val sizeCounter = AtomicLong(0)
    private val lastUpdatedTimestamp = AtomicLong(0)
    private val oDataFilterService = ODataFilterService()

    // Retained for close() — these are native objects that must be explicitly released
    // sharedBlockCache is owned by CacheService, not closed here
    private val columnFamilyOptions: ColumnFamilyOptions
    private val dbOptions: DBOptions

    init {
        val dir = File(dbPath)
        dir.deleteRecursively()
        check(dir.mkdirs()) { "Failed to create RocksDB directory: $dbPath" }

        columnFamilyOptions =
            ColumnFamilyOptions()
                .setTableFormatConfig(BlockBasedTableConfig().setBlockCache(sharedBlockCache))
                .setWriteBufferSize(writeBufferSizeBytes)
                .setMaxWriteBufferNumber(2)
        dbOptions =
            DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setDbWriteBufferSize(totalWriteBufferSizeBytes)

        val cfDescriptors =
            listOf(
                ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                ColumnFamilyDescriptor("sorted".toByteArray(), columnFamilyOptions),
                ColumnFamilyDescriptor("index".toByteArray(), columnFamilyOptions),
            )
        val cfHandles = mutableListOf<ColumnFamilyHandle>()

        db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles)
        entriesCf = cfHandles[0]
        sortedCf = cfHandles[1]
        indexCf = cfHandles[2]
    }

    fun put(
        resourceId: String,
        resource: T,
        timestamp: Long,
    ) {
        val ridBytes = resourceId.toByteArray(Charsets.UTF_8)
        writeLock.withLock {
            val existingBytes = db.get(entriesCf, ridBytes)
            if (existingBytes != null) {
                val existingTimestamp = ByteBuffer.wrap(existingBytes, 0, 8).getLong()
                if (timestamp < existingTimestamp) return@withLock
                val existingResource = decodeEntry(existingBytes).second
                WriteBatch().use { batch ->
                    batch.delete(sortedCf, encodeSortedKey(existingTimestamp, resourceId))
                    removeFromIndexBatch(batch, existingResource)
                    batch.put(entriesCf, ridBytes, encodeEntry(resource, timestamp))
                    batch.put(sortedCf, encodeSortedKey(timestamp, resourceId), ridBytes)
                    addToIndexBatch(batch, resource, ridBytes)
                    db.write(writeOptions, batch)
                }
            } else {
                WriteBatch().use { batch ->
                    batch.put(entriesCf, ridBytes, encodeEntry(resource, timestamp))
                    batch.put(sortedCf, encodeSortedKey(timestamp, resourceId), ridBytes)
                    addToIndexBatch(batch, resource, ridBytes)
                    db.write(writeOptions, batch)
                }
                sizeCounter.incrementAndGet()
            }
            lastUpdated = timestamp
        }
    }

    fun get(resourceId: String): T? {
        val bytes = db.get(entriesCf, resourceId.toByteArray(Charsets.UTF_8)) ?: return null
        return decodeEntry(bytes).second
    }

    fun lastUpdatedByResourceId(resourceId: String): Long? {
        val bytes = db.get(entriesCf, resourceId.toByteArray(Charsets.UTF_8)) ?: return null
        return ByteBuffer.wrap(bytes, 0, 8).getLong()
    }

    fun getByIdField(
        field: String,
        value: Any,
    ): T? {
        val ridBytes = db.get(indexCf, encodeIndexKey(field, value)) ?: return null
        val entryBytes = db.get(entriesCf, ridBytes) ?: return null
        return decodeEntry(entryBytes).second
    }

    fun getList(
        size: Long,
        offset: Long,
        sinceTimestamp: Long,
        filter: String?,
    ): List<T> {
        val hasFilter = !filter.isNullOrBlank()
        val iter = db.newIterator(sortedCf)
        if (sinceTimestamp > 0L) {
            // Seek to first key with timestamp >= sinceTimestamp.
            // An 8-byte key (no resourceId suffix) sorts before any real entry at that
            // timestamp, so all entries at exactly sinceTimestamp are included.
            iter.seek(ByteBuffer.allocate(8).putLong(sinceTimestamp).array())
        } else {
            iter.seekToFirst()
        }

        // Fast path: no filter + bounded page — skip entries without deserializing
        if (!hasFilter && size > 0) {
            return try {
                var skipped = 0L
                while (iter.isValid && skipped < offset) {
                    iter.next()
                    skipped++
                }
                val results = mutableListOf<T>()
                while (iter.isValid && results.size.toLong() < size) {
                    db.get(entriesCf, iter.value())?.let { results.add(decodeEntry(it).second) }
                    iter.next()
                }
                results
            } finally {
                iter.close()
            }
        }

        // Lazy stream path: OData filter or unbounded — deserializes one entry at a time
        val baseStream = lazyResourceStream(iter)
        return try {
            var s: Stream<T> = baseStream
            if (hasFilter) s = applyODataFilter(s, filter!!)
            if (size > 0) {
                if (offset > 0) s = s.skip(offset)
                s = s.limit(size)
            }
            s.toList()
        } finally {
            baseStream.close()
        }
    }

    fun remove(
        resourceId: String,
        timestamp: Long,
    ) {
        val ridBytes = resourceId.toByteArray(Charsets.UTF_8)
        writeLock.withLock {
            val existingBytes = db.get(entriesCf, ridBytes) ?: return@withLock
            val (existingTimestamp, existingResource) = decodeEntry(existingBytes)
            if (timestamp <= existingTimestamp) return@withLock
            WriteBatch().use { batch ->
                batch.delete(entriesCf, ridBytes)
                batch.delete(sortedCf, encodeSortedKey(existingTimestamp, resourceId))
                removeFromIndexBatch(batch, existingResource)
                db.write(writeOptions, batch)
            }
            sizeCounter.decrementAndGet()
            lastUpdated = timestamp
        }
    }

    fun evictExpired(timestamp: Long): Set<Pair<String, T>> {
        data class EvictEntry(
            val sortedKeyBytes: ByteArray,
            val resourceId: String,
            val resource: T,
        )

        return writeLock.withLock {
            val toEvict = mutableListOf<EvictEntry>()
            db.newIterator(sortedCf).use { iter ->
                iter.seekToFirst()
                while (iter.isValid) {
                    val sortedKeyBytes = iter.key()
                    // Stop as soon as we reach entries at or after the eviction boundary
                    if (ByteBuffer.wrap(sortedKeyBytes, 0, 8).getLong() >= timestamp) break
                    val ridBytes = iter.value()
                    db.get(entriesCf, ridBytes)?.let {
                        toEvict.add(
                            EvictEntry(
                                sortedKeyBytes = sortedKeyBytes.clone(),
                                resourceId = String(ridBytes, Charsets.UTF_8),
                                resource = decodeEntry(it).second,
                            ),
                        )
                    }
                    iter.next()
                }
            }

            if (toEvict.isEmpty()) return@withLock emptySet<Pair<String, T>>()

            WriteBatch().use { batch ->
                for (entry in toEvict) {
                    batch.delete(entriesCf, entry.resourceId.toByteArray(Charsets.UTF_8))
                    batch.delete(sortedCf, entry.sortedKeyBytes)
                    removeFromIndexBatch(batch, entry.resource)
                }
                db.write(writeOptions, batch)
            }
            sizeCounter.addAndGet(-toEvict.size.toLong())
            toEvict.map { Pair(it.resourceId, it.resource) }.toSet()
        }
    }

    var lastUpdated: Long
        get() = lastUpdatedTimestamp.get()
        private set(value) {
            lastUpdatedTimestamp.accumulateAndGet(value) { existing, new -> max(existing, new) }
        }

    val size: Int
        get() = sizeCounter.get().toInt()

    override fun close() {
        writeOptions.close()
        indexCf.close()
        sortedCf.close()
        entriesCf.close()
        db.close()
        dbOptions.close()
        columnFamilyOptions.close()
        // sharedBlockCache is owned by CacheService — not closed here
    }

    // Creates a lazy Stream backed by a RocksDB iterator. The iterator is closed via
    // stream.close(), which must be called by the caller (use try-finally or onClose).
    private fun lazyResourceStream(iter: RocksIterator): Stream<T> =
        StreamSupport
            .stream(
                object : Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.ORDERED) {
                    override fun tryAdvance(action: Consumer<in T>): Boolean {
                        while (iter.isValid) {
                            val bytes = db.get(entriesCf, iter.value())
                            iter.next()
                            if (bytes != null) {
                                action.accept(decodeEntry(bytes).second)
                                return true
                            }
                        }
                        return false
                    }
                },
                false,
            ).onClose(iter::close)

    private fun encodeEntry(
        resource: T,
        timestamp: Long,
    ): ByteArray {
        val classNameBytes = resource.javaClass.name.toByteArray(Charsets.UTF_8)
        val jsonBytes = objectMapper.writeValueAsBytes(resource)
        return ByteBuffer
            .allocate(8 + 4 + classNameBytes.size + jsonBytes.size)
            .putLong(timestamp)
            .putInt(classNameBytes.size)
            .put(classNameBytes)
            .put(jsonBytes)
            .array()
    }

    @Suppress("UNCHECKED_CAST")
    private fun decodeEntry(bytes: ByteArray): Pair<Long, T> {
        val buf = ByteBuffer.wrap(bytes)
        val timestamp = buf.getLong()
        val classNameBytes = ByteArray(buf.getInt()).also { buf.get(it) }
        val jsonBytes = ByteArray(buf.remaining()).also { buf.get(it) }
        val resource = objectMapper.readValue(jsonBytes, Class.forName(String(classNameBytes, Charsets.UTF_8))) as T
        return Pair(timestamp, resource)
    }

    private fun encodeSortedKey(
        timestamp: Long,
        resourceId: String,
    ): ByteArray {
        val ridBytes = resourceId.toByteArray(Charsets.UTF_8)
        return ByteBuffer
            .allocate(8 + ridBytes.size)
            .putLong(timestamp)
            .put(ridBytes)
            .array()
    }

    private fun encodeIndexKey(
        idKey: String,
        idValue: Any,
    ): ByteArray = "${idKey.lowercase()} $idValue".toByteArray(Charsets.UTF_8)

    private fun addToIndexBatch(
        batch: WriteBatch,
        resource: T,
        ridBytes: ByteArray,
    ) {
        resource.identifikators
            .filter { it.value?.identifikatorverdi != null }
            .forEach { (key, value) ->
                batch.put(indexCf, encodeIndexKey(key, value.identifikatorverdi), ridBytes)
            }
    }

    private fun removeFromIndexBatch(
        batch: WriteBatch,
        resource: T,
    ) {
        resource.identifikators
            .filter { it.value?.identifikatorverdi != null }
            .forEach { (key, value) ->
                batch.delete(indexCf, encodeIndexKey(key, value.identifikatorverdi))
            }
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
}
