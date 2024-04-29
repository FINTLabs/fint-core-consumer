package no.fintlabs.cache;

import no.fint.model.FintResource;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class CoreCache<T extends FintResource> implements Cache<T> {

    private final Map<String, T> resourceCache = new LinkedHashMap<>();
    private final NavigableMap<Long, Set<String>> lastUpdatedIndex = new TreeMap<>();
    private long retentionTimeInMs;
    private long lastUpdated = 0;

    public T get(String idValue) {
        return resourceCache.get(idValue);
    }

    @Override
    public void put(String key, T resource) {
        resourceCache.put(key, resource);
        lastUpdated = System.currentTimeMillis();
        lastUpdatedIndex.computeIfAbsent(lastUpdated, k -> new HashSet<>()).add(key);
    }

    @Override
    public void remove(String key) {
        if (resourceCache.containsKey(key)) {
            resourceCache.remove(key);
            lastUpdatedIndex.values().forEach(set -> set.remove(key));
        }
        lastUpdated = System.currentTimeMillis();
    }

    @Override
    public void flush() {
        resourceCache.clear();
        lastUpdatedIndex.clear();
        lastUpdated = System.currentTimeMillis();
    }

    @Override
    public long getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public int size() {
        return resourceCache.size();
    }

    @Override
    public boolean empty() {
        return resourceCache.isEmpty();
    }

    @Override
    public Stream<T> stream() {
        return resourceCache.values().stream();
    }

    @Override
    public Stream<T> streamSince(long timestamp) {
        return lastUpdatedIndex.tailMap(timestamp, true).values().stream()
                .flatMap(Set::stream)
                .map(resourceCache::get);
    }

    @Override
    public Stream<T> streamSlice(int skip, int limit) {
        return stream().skip(skip).limit(limit);
    }

    @Override
    public Stream<T> streamSliceSince(long sinceTimeStamp, int skip, int limit) {
        return streamSince(sinceTimeStamp).skip(skip).limit(limit);
    }

    // TODO: Improve this
    // Does this need to exist?
    @Override
    public Optional<T> getLastUpdatedByFilter(int hashCode, Predicate<T> predicate) {
        return resourceCache.values().stream()
                .filter(resource -> resource.hashCode() == hashCode && predicate.test(resource))
                .max(Comparator.comparingLong(resource -> lastUpdated));
    }

    @Override
    public void evictOldCacheObjects() {
        long cutoff = System.currentTimeMillis() - retentionTimeInMs;
        lastUpdatedIndex.headMap(cutoff, false).values().stream()
                .flatMap(Set::stream)
                .forEach(resourceCache::remove);
        lastUpdatedIndex.headMap(cutoff, false).clear();
    }

    @Override
    public void setRetentionPeriodInMs(long periodInMs) {
        retentionTimeInMs = periodInMs;
    }

}
