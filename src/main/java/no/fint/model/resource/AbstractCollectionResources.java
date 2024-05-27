package no.fint.model.resource;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.*;
import no.fint.model.resource.Link;

import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public abstract class AbstractCollectionResources<T> {
    protected AbstractCollectionResources(Collection<T> input) {
        this();
        embedded.entries.addAll(input);
    }

    @JsonProperty("_embedded")
    protected EmbeddedResources<T> embedded = new EmbeddedResources<>();

    @Getter
    @JsonProperty("_links")
    protected final Map<String, List<Link>> links = new LinkedHashMap<>();

    private int totalItems;

    @JsonSetter("total_items")
    public void setTotalItems(int totalItems) {
        this.totalItems = totalItems;
    }

    @JsonGetter("total_items")
    public int getTotalItems() {
        totalItems = Math.max(embedded.entries.size(), totalItems);
        return totalItems;
    }

    @JsonGetter("size")
    public int getSize() {
        return embedded.entries.size();
    }

    @Setter
    @Getter
    @JsonProperty("offset")
    private int offset;

    public void addResource(T resource) {
        embedded.entries.add(resource);
    }

    protected void addLink(String key, Link link) {
        getLinks().computeIfAbsent(key, (k) -> new ArrayList<>()).add(link);
    }

    public void addSelf(Link link) {
        addLink("self", link);
    }

    public void addNext(Link link) {
        addLink("next", link);
    }

    public void addPrev(Link link) {
        addLink("prev", link);
    }

    @JsonIgnore
    public List<Link> getSelfLinks() {
        return links.get("self");
    }

    @JsonIgnore
    public List<T> getContent() {
        return embedded.entries;
    }

    @Deprecated
    @JsonIgnore
    public abstract TypeReference<List<T>> getTypeReference();

    @Data
    @NoArgsConstructor
    public static class EmbeddedResources<T> {
        @JsonProperty("_entries")
        public List<T> entries = new ArrayList<>();
    }
}
