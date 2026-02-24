package no.fintlabs.consumer.resource

import no.novari.fint.model.resource.FintResource
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
     * Acquires a lock for the given [key], executes [block], then releases the lock.
     *
     * @param key Identifies the resource to lock.
     * @param block The operation to perform while the lock is held.
     * @return The result of [block].
     */
    fun withLock(
        key: ResourceLockKey,
        block: () -> FintResource,
    ): FintResource {
        val lock = locks.computeIfAbsent(key) { ReentrantLock() }
        lock.lock()
        return try {
            block()
        } finally {
            lock.unlock()
        }
    }
}
