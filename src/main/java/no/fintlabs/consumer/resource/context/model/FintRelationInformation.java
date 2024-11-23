package no.fintlabs.consumer.resource.context.model;

import no.fint.model.FintModelObject;

import java.util.Set;
import java.util.stream.Collectors;

public record FintRelationInformation(
        String name,
        String packageName,
        Set<String> idFields,
        String uri,
        boolean isReference
) {

    public static FintRelationInformation byModelObject(String name, String packageName, FintModelObject fintModelObject, String uri, boolean isReference) {
        return new FintRelationInformation(
                name.toLowerCase(),
                packageName,
                fintModelObject.getIdentifikators().keySet().stream()
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet()),
                uri,
                isReference
        );
    }

}
