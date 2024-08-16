package no.fintlabs.reflection;

import no.fint.model.FintRelation;
import no.fint.model.resource.FintResource;

import java.util.List;
import java.util.Set;

public record FintResourceInformation(
        Class<? extends FintResource> clazz,
        Set<String> idFieldNames,
        List<FintRelation> relations,
        boolean isWriteable
) {
}
