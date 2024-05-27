package no.fintlabs.consumer;

import jakarta.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;

@Service
@RequiredArgsConstructor
public class LinkService {

    @Value("${fint.relation.base-url}")
    private String baseUrl;

    private final ConsumerConfiguration configuration;

    public void removePlaceholders(FintResource fintResource) {
        if (fintResource != null) {
            fintResource.getLinks().forEach((resourceName, links) -> {
                links.forEach(this::removePlaceholder);
            });
        }
    }

    private void removePlaceholder(Link link) {
        link.setVerdi(link.getHref().replaceAll("\\$\\{[^}]*}", ""));
    }

    @Nullable
    public URI createSelfHref(String resourceName, FintResource resource) {
        return resource.getIdentifikators().entrySet().stream()
                .filter(e -> e.getValue().getIdentifikatorverdi() != null)
                .findFirst()
                .map(e -> URI.create(String.format("%s/%s/%s/%s/%s/%s",
                        baseUrl,
                        configuration.getDomain(),
                        configuration.getPackageName(),
                        resourceName,
                        e.getKey().toLowerCase(),
                        e.getValue().getIdentifikatorverdi())))
                .orElse(null);
    }

    public URI createSelfHref(RequestFintEvent requestFintEvent) {
        return URI.create("%s/%s/%s/%s/status/%s".formatted(
                baseUrl,
                configuration.getDomain(),
                configuration.getPackageName(),
                requestFintEvent.getResourceName(),
                requestFintEvent.getCorrId()
        ));
    }

}
