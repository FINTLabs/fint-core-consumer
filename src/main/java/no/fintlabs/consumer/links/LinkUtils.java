package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
public class LinkUtils {

    private final ConsumerConfiguration config;

    public String[] createSelfHrefs(String resourceName, FintResource resource) {
        return resource.getIdentifikators().entrySet().stream()
                .filter(entrySet -> entrySet.getValue().getIdentifikatorverdi() != null)
                .map(entrySet -> String.format("%s/%s/%s/%s",
                        config.getComponentUrl(),
                        resourceName,
                        entrySet.getKey().toLowerCase(),
                        entrySet.getValue().getIdentifikatorverdi()))
                .toArray(String[]::new);
    }

    public String getFirstSelfHref(FintResource resource) {
        return resource.getSelfLinks().getFirst().getHref();
    }

    public String getStatusHref(RequestFintEvent requestFintEvent) {
        return "%s/%s/status/%s".formatted(
                config.getComponentUrl(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getCorrId()
        );
    }

}
