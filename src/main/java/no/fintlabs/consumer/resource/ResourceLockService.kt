package no.fintlabs.consumer.resource

import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

data class ResourceLockKey(
    val resourceName: String,
    val uniqueIdentifier: String,
)

/**
 * Manages reentrant locks for resources, ensuring that only one thread can
 * operate on a given [ResourceLockKey] at a time.
 */
@Service
class ResourceLockService {
    private val locks: ConcurrentHashMap<ResourceLockKey, ReentrantLock> = ConcurrentHashMap()

    /**
     * Acquires a lock for [resourceName] + [uniqueIdentifier], executes [block], then releases the lock.
     *
     * @param resourceName The name of the resource type.
     * @param uniqueIdentifier The unique identifier for the resource instance.
     * @param block The operation to perform while the lock is held.
     */
    fun withLock(
        resourceName: String,
        uniqueIdentifier: String,
        block: () -> Unit,
    ) {
        val lock = locks.computeIfAbsent(ResourceLockKey(resourceName, uniqueIdentifier)) { ReentrantLock() }
        lock.lock()
        try {
            block()
        } finally {
            lock.unlock()
        }
    }
}
