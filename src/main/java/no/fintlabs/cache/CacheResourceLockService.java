package no.fintlabs.cache;

import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

@Service
public class CacheResourceLockService {

    private final ConcurrentHashMap<String, ReentrantLock> resourceLocks = new ConcurrentHashMap<>();

    public void withLock(String resourceName, Runnable runnable) {
        withLock(resourceName, () -> {
            runnable.run();
            return null;
        });
    }

    public <T> T withLock(String resourceName, Supplier<T> supplier) {
        ReentrantLock lock = resourceLocks.computeIfAbsent(normalize(resourceName), ignored -> new ReentrantLock());
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    private String normalize(String resourceName) {
        return resourceName == null ? "" : resourceName.toLowerCase(Locale.ROOT);
    }
}
