package no.fintlabs.consumer.controllers;

import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static no.fintlabs.consumer.config.Endpoints.COMPONENT_BASE_URL;

@Data
@Builder
public class EndpointDetails {

    private final String collectionUrl;
    private final List<String> oneUrl;
    private final String cacheSizeUrl;
    private final String lastUpdatedUrl;

    public static EndpointDetails ofResource(String resource, Set<String> idFields) {
        return EndpointDetails.builder()
                .collectionUrl(COMPONENT_BASE_URL + String.format("/%s", resource))
                .oneUrl(setOneUrl(resource, idFields))
                .cacheSizeUrl(COMPONENT_BASE_URL + String.format("/%s/cache/size", resource))
                .lastUpdatedUrl(COMPONENT_BASE_URL + String.format("/%s/last-updated", resource))
                .build();
    }

    private static List<String> setOneUrl(String resource, Set<String> idFields) {
        List<String> oneUrls = new ArrayList<>();

        idFields.forEach(idField -> {
            oneUrls.add(COMPONENT_BASE_URL + String.format("/%s/%s/{id:.+}", resource, idField));
        });

        return oneUrls;
    }

}
