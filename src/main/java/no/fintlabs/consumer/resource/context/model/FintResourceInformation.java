package no.fintlabs.consumer.resource.context.model;

import no.fint.model.FintModelObject;
import no.fint.model.resource.FintResource;

import java.util.Map;
import java.util.Set;

public record FintResourceInformation(
        String name,
        Class<? extends FintResource> clazz,
        Set<String> idFieldNames,
        boolean isWriteable,
        Set<String> referenceNames,
        Set<String> requiredRelations,
        Set<String> abstractRelations,
        Map<String, FintRelationInformation> relations
) {

    public static FintResourceInformation byMetaSubType(
            Class<? extends FintModelObject> metaSubType,
            Class<? extends FintResource> resourceClass,
            FintModelObject fintModelObject,
            Set<String> referenceNames,
            Set<String> requiredRelations,
            Set<String> abstractRelations,
            Map<String, FintRelationInformation> relations
    ) {
        return new FintResourceInformation(
                metaSubType.getSimpleName().toLowerCase(),
                resourceClass,
                fintModelObject.getIdentifikators().keySet(),
                fintModelObject.isWriteable(),
                referenceNames,
                requiredRelations,
                abstractRelations,
                relations
        );
    }

}
