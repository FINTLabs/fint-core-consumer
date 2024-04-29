package no.fintlabs.cache;

import no.fint.model.FintResource;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface Cache<T extends FintResource> {
    void put(String key, T object);

    void remove(String key);

    void flush();

    long getLastUpdated();

    int size();

    boolean empty();

    Stream<T> stream();

    Stream<T> streamSince(long timestamp);

    Stream<T> streamSlice(int skip, int limit);

    Stream<T> streamSliceSince(long sinceTimeStamp, int skip, int limit);

    Optional<T> getLastUpdatedByFilter(int hashCode, Predicate<T> predicate);

    void evictOldCacheObjects();

    void setRetentionPeriodInMs(long periodInMs);

}

