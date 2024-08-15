package no.fintlabs.consumer.links;

import jakarta.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import no.fint.model.FintIdentifikator;
import no.fint.model.resource.FintResource;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.springframework.stereotype.Component;

import java.util.Map;


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

    @Nullable
    public String createFirstSelfHref(String resourceName, FintResource resource) {
        for (Map.Entry<String, FintIdentifikator> entry : resource.getIdentifikators().entrySet()) {
            if (entry.getValue() != null && entry.getValue().getIdentifikatorverdi() != null) {
                return "%s/%s/%s/%s".formatted(
                        config.getComponentUrl(),
                        resourceName,
                        entry.getKey().toLowerCase(),
                        entry.getValue().getIdentifikatorverdi()
                );
            }
        }

        return null;
    }

    public String getStatusHref(RequestFintEvent requestFintEvent) {
        return "%s/%s/status/%s".formatted(
                config.getComponentUrl(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getCorrId()
        );
    }

}
