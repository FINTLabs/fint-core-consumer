package no.fintlabs.cache;

import no.fint.model.FintResource;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CacheContainer {

    private final Map<String, Map<String, CoreCache<FintResource>>> resourceCache = new ConcurrentHashMap<>();

    public CoreCache<FintResource> getCache(String resource, String idField) {
        return resourceCache.get(resource).get(idField);
    }

    public void initializeCache(String resource, String idField) {
        resourceCache.putIfAbsent(resource, new ConcurrentHashMap<>());
        resourceCache.get(resource).put(idField, new CoreCache<>());
    }

}
