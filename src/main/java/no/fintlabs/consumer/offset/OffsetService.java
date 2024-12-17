package no.fintlabs.consumer.offset;

import lombok.Getter;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Getter
@Service
public class OffsetService {

    private final Map<String, Long> offsetMap;

    public OffsetService(ResourceContext resourceContext) {
        this.offsetMap = createOffsetMap(resourceContext);
    }

    public void updateEntityOffset(String resourceName, Long offset) {
        offsetMap.put(createEntityOffsetKey(resourceName), offset);
    }

    public void updateRequestOffset(String resourceName, Long offset) {
        offsetMap.put(createRequestOffsetKey(resourceName), offset);
    }

    public void updateResponseOffset(String resourceName, Long offset) {
        offsetMap.put(createResponseOffsetKey(resourceName), offset);
    }

    private Map<String, Long> createOffsetMap(ResourceContext resourceContext) {
        HashMap<String, Long> offsetMap = new HashMap<>();

        resourceContext.getResources().forEach(resource -> {
            offsetMap.put(createEntityOffsetKey(resource.name()), -1L);
            if (resource.isWriteable()) {
                offsetMap.put(createRequestOffsetKey(resource.name()), -1L);
                offsetMap.put(createResponseOffsetKey(resource.name()), -1L);
            }
        });

        return offsetMap;
    }

    private String createRequestOffsetKey(String resourceName) {
        return "%sRequestOffset".formatted(resourceName);

    }

    private String createResponseOffsetKey(String resourceName) {
        return "%sResponseOffset".formatted(resourceName);
    }

    private String createEntityOffsetKey(String resourceName) {
        return "%sOffset".formatted(resourceName);
    }

}
