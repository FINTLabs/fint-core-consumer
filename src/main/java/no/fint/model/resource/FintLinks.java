package no.fint.model.resource;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.*;

public interface FintLinks {
    Map<String, List<no.fint.model.resource.Link>> getLinks();

    @JsonGetter("_links")
    default Map<String, List<no.fint.model.resource.Link>> getLinksIfPresent() {
        if (getLinks().isEmpty()) {
            return null;
        }
        return getLinks();
    }

    @JsonSetter("_links")
    default void setLinks(Map<String, List<no.fint.model.resource.Link>> links) {
        if (links != null) {
            getLinks().putAll(links);
        }
    }

    default void addLink(String key, no.fint.model.resource.Link link) {
        getLinks().computeIfAbsent(key, (k) -> new ArrayList<>()).add(link);
    }

    default void addSelf(no.fint.model.resource.Link link) { addLink("self", link); }

    default Map<String, List<no.fint.model.resource.Link>> createLinks() {
        return Collections.synchronizedMap(new LinkedHashMap<>());
    }

    @JsonIgnore
    default List<FintLinks> getNestedResources() {
        return new ArrayList<>();
    }

    @JsonIgnore
    default List<Link> getSelfLinks() {
        return getLinks().get("self");
    }
}
