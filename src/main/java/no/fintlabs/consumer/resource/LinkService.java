package no.fintlabs.consumer.resource;

import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class LinkService {

    @Value("${fint.relation.base-url}")
    private String baseUrl;

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

}
