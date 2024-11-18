package no.fintlabs.consumer.links.nested;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintLinks;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.apache.commons.text.StringSubstitutor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Slf4j
@Service
public class NestedLinkService {

    private final ConsumerConfiguration configuration;
    private final StringSubstitutor stringSubstitutor;

    public NestedLinkService(ConsumerConfiguration configuration, NestedLinkMapper nestedLinkMapper) {
        this.configuration = configuration;
        this.stringSubstitutor = new StringSubstitutor(nestedLinkMapper.getPackageToUriMap());
    }

    public void mapNestedLinks(FintLinks resource) {
        resource.getNestedResources().forEach(fintLinks -> {
            mapLinks(fintLinks);
            mapNestedLinks(fintLinks);
        });
    }

    private void mapLinks(FintLinks fintLinks) {
        fintLinks.getLinks().values().stream()
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .filter(Objects::nonNull)
                .forEach(link -> link.setVerdi(getLink(link.getHref())));
    }

    public String getLink(String link) {
        if (link.startsWith("${") && link.contains("}")) {
            link = stringSubstitutor.replace(link);
            return "%s/%s".formatted(configuration.getBaseUrl(), link);
        }

        if (link.startsWith("/")) {
            return configuration.getBaseUrl() + link;
        }

        return populateProtocol(link);
    }

    public String populateProtocol(String href) {
        if (href.startsWith("http://")) {
            return href.replace("http://", "https://");
        } else {
            return href;
        }
    }


}
