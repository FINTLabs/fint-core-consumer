package no.fintlabs.reflection;

import no.fint.model.FintModelObject;
import no.fint.model.FintRelation;
import no.fint.model.resource.FintResource;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public record FintResourceInformation(
        String name,
        Class<? extends FintResource> clazz,
        Set<String> idFieldNames,
        List<FintRelation> relations,
        boolean isWriteable
) {

    public static FintResourceInformation byMetaData(String resourceName, Class<? extends FintResource> resourceClass, FintModelObject fintModelObject) {
        return new FintResourceInformation(
                resourceName,
                resourceClass,
                fintModelObject.getIdentifikators().keySet().stream().map(String::toLowerCase).collect(Collectors.toSet()),
                fintModelObject.getRelations(),
                fintModelObject.isWriteable()
        );
    }

}
