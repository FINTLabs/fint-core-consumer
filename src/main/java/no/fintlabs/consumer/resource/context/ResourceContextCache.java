package no.fintlabs.consumer.resource.context;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintModelObject;
import no.fint.model.FintMultiplicity;
import no.fint.model.FintRelation;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.context.model.FintRelationInformation;
import no.fintlabs.consumer.resource.context.model.FintResourceInformation;
import no.fintlabs.reflection.ReflectionCache;
import no.fintlabs.reflection.ReflectionInitializer;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ResourceContextCache {

    private final ConsumerConfiguration configuration;
    private final ReflectionCache reflectionCache;
    private final ReflectionInitializer reflectionInitializer;

    protected final Map<String, FintResourceInformation> resourceMap = new HashMap<>();

    @PostConstruct
    private void init() {
        fillResourceMap();
    }

    private void fillResourceMap() {
        reflectionCache.getAllMetaSubtypes().stream()
                .filter(this::metaSubTypeBelongsToThisComponent)
                .map(this::createFintResourceInformation)
                .forEach(resource -> {
                    addResourceInformation(resource);
                    checkRelationsForCommonResources(resource.relations().values());
                });
    }

    private FintResourceInformation createFintResourceInformation(Class<? extends FintModelObject> metaSubType) {
        FintModelObject fintModelObject = reflectionInitializer.initializeFintModelObject(metaSubType);
        return FintResourceInformation.byMetaSubType(
                metaSubType,
                reflectionCache.getResourceSubtype(metaSubType.getName()),
                fintModelObject,
                getReferenceNames(fintModelObject),
                getRequiredRelations(fintModelObject),
                getAbstractRelations(fintModelObject),
                createFintRelationInformations(fintModelObject)
        );
    }

    private Map<String, FintRelationInformation> createFintRelationInformations(FintModelObject fintModelObject) {
        return fintModelObject.getRelations().stream()
                .filter(relation -> !reflectionCache.isAnAbstractObject(relation.getPackageName()))
                .map(this::createFintRelationInformation)
                .collect(Collectors.toMap(
                        FintRelationInformation::name,
                        relation -> relation
                ));
    }

    private void checkRelationsForCommonResources(Collection<FintRelationInformation> fintRelations) {
        fintRelations.stream()
                .filter(relation -> isACommonResource(relation.packageName()))
                .map(relation -> reflectionCache.getMetaSubtype(relation.packageName()))
                .map(this::createFintResourceInformation)
                .forEach(this::addResourceInformation);
    }

    private FintRelationInformation createFintRelationInformation(FintRelation fintRelation) {
        Class<? extends FintModelObject> metaSubtype = reflectionCache.getMetaSubtype(fintRelation.getPackageName());
        return FintRelationInformation.byModelObject(
                fintRelation.getName(),
                fintRelation.getPackageName(),
                reflectionInitializer.initializeFintModelObject(metaSubtype),
                createRelationUri(fintRelation.getPackageName(), fintRelation.getName()),
                reflectionCache.isAReference(fintRelation.getPackageName())
        );
    }

    private String createRelationUri(String packageName, String relationName) {
        if (isACommonResource(packageName)) {
            return "%s/%s/%s".formatted(
                    configuration.getDomain(),
                    configuration.getPackageName(),
                    relationName.toLowerCase()
            );
        } else {
            return packageName.replaceFirst("no.fint.model.", "")
                    .replace(".", "/")
                    .toLowerCase();
        }
    }

    private Set<String> getRequiredRelations(FintModelObject fintModelObject) {
        return fintModelObject.getRelations().stream()
                .filter(relation -> relation.getMultiplicity().equals(FintMultiplicity.ONE_TO_ONE))
                .map(FintRelation::getName)
                .collect(Collectors.toSet());
    }

    private Set<String> getReferenceNames(FintModelObject fintModelObject) {
        return fintModelObject.getRelations().stream()
                .filter(relation -> reflectionCache.isAReference(relation.getPackageName()))
                .map(relation -> relation.getName().toLowerCase())
                .collect(Collectors.toSet());
    }

    private Set<String> getAbstractRelations(FintModelObject fintModelObject) {
        return fintModelObject.getRelations().stream()
                .filter(relation -> reflectionCache.isAnAbstractObject(relation.getPackageName()))
                .map(relation -> relation.getName().toLowerCase())
                .collect(Collectors.toSet());
    }

    private boolean isACommonResource(String packageName) {
        return packageName.split("\\.").length == 5;
    }

    private boolean metaSubTypeBelongsToThisComponent(Class<? extends FintModelObject> metaSubType) {
        return metaSubType.getPackageName().toLowerCase().contains(
                "%s.%s".formatted(
                        configuration.getDomain(),
                        configuration.getPackageName()).toLowerCase()
        );
    }

    private void addResourceInformation(FintResourceInformation fintResourceInformation) {
        resourceMap.put(fintResourceInformation.name(), fintResourceInformation);
    }

}
