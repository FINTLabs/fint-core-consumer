package no.fintlabs.consumer.resource.context.model;

import no.novari.fint.model.FintModelObject;

import java.util.Set;
import java.util.stream.Collectors;

public record FintRelationInformation(
        String name,
        String packageName,
        Set<String> idFields,
        String uri
) {

    public static FintRelationInformation byModelObject(String name, String packageName, FintModelObject fintModelObject, String uri) {
        return new FintRelationInformation(
                name.toLowerCase(),
                packageName,
                fintModelObject.getIdentifikators().keySet().stream()
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet()),
                uri
        );
    }

}
