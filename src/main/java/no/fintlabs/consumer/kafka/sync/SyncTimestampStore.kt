package no.fintlabs.consumer.kafka.sync

import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * Tracks the most recent successfully completed full sync timestamp for each resource.
 *
 * Example:
 * ```
 * store.recordFullSync("employee", 1710758400000L)
 * store.getLastFullSync("employee") // -> 2024-03-18T08:00:00Z
 * store.getLastFullSync("unknown")  // -> null
 * ```
 */
@Component
class SyncTimestampStore {
    private val lastFullSync: ConcurrentHashMap<String, Instant> = ConcurrentHashMap()

    fun recordFullSync(
        resourceName: String,
        timestamp: Long,
    ) = lastFullSync.put(resourceName, Instant.ofEpochMilli(timestamp))

    fun getLastFullSync(resourceName: String): Instant? = lastFullSync[resourceName]
}
