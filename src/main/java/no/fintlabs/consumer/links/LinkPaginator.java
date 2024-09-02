package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

@Component
@RequiredArgsConstructor
public class LinkPaginator {

    private final ConsumerConfiguration configuration;

    public void addPagination(String resourceName, FintResources resources, int offset, int size, int totalItems) {
        String baseUri = self(resourceName);
        UriComponentsBuilder baseBuilder = UriComponentsBuilder.fromUriString(baseUri);

        if (size > 0) {
            addLink(resources, "self", baseBuilder, offset, size);

            if (offset > 0) {
                int prevOffset = Math.max(0, offset - size);
                addLink(resources, "prev", baseBuilder, prevOffset, size);
            }

            if (offset + size < totalItems) {
                int nextOffset = offset + size;
                addLink(resources, "next", baseBuilder, nextOffset, size);
            }
        } else {
            resources.addSelf(Link.with(baseUri));
        }

        resources.setOffset(offset);
        resources.setTotalItems(totalItems);
    }

    private void addLink(FintResources resources, String rel, UriComponentsBuilder builder, int offset, int size) {
        String uri = builder.replaceQueryParam("offset", offset)
                .replaceQueryParam("size", size)
                .toUriString();

        switch (rel) {
            case "self":
                resources.addSelf(Link.with(uri));
                break;
            case "prev":
                resources.addPrev(Link.with(uri));
                break;
            case "next":
                resources.addNext(Link.with(uri));
                break;
        }
    }

    public String self(String resourceName) {
        return "%s/%s".formatted(configuration.getComponentUrl(), resourceName);
    }

}
