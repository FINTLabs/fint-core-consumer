package no.fintlabs.reflection;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintModelObject;
import no.fint.model.FintRelation;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.model.FintRelationInformation;
import no.fintlabs.reflection.model.FintResourceInformation;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class ResourceContextCache {

    private final ReflectionService reflectionService;
    private final ConsumerConfiguration configuration;

    protected final Set<String> abstractPackages = new HashSet<>();
    protected final Set<String> referencePackages = new HashSet<>();
    protected final Map<String, Set<String>> resourceReferencesMap = new HashMap<>();
    protected final Map<String, FintResourceInformation> resourceToResourceInformationMap = new HashMap<>();
    protected final Map<String, FintRelationInformation> packageToRelationInformationMap = new HashMap<>();

    @PostConstruct
    private void init() {
        fillAbstractPackages();
        fillReferencePackages();
        fillResourceInformationMap();
        fillRelationInformationMap();
        fillResourceReferencesMap();
        crashIfNoResourcesAreFound();
    }

    private void fillResourceReferencesMap() {
        resourceToResourceInformationMap.values().forEach(resource ->
                resource.relations().forEach(relation -> {
                    if (referencePackages.contains(relation.getPackageName())) {
                        resourceReferencesMap.computeIfAbsent(resource.name().toLowerCase(), key -> new HashSet<>())
                                .add(relation.getName().toLowerCase());
                    }
                })
        );
    }

    private void fillReferencePackages() {
        reflectionService.getFintReferenceSubTypes().forEach(subType -> referencePackages.add(subType.getName()));
    }

    private void fillAbstractPackages() {
        reflectionService.getFintAbstractSubTypes().forEach(subType -> abstractPackages.add(subType.getName()));
    }

    private void fillResourceInformationMap() {
        reflectionService.getPackageMetaSubTypeMap().values().forEach(metaSubType -> {
            if (metaSubTypeBelongsToThisComponent(metaSubType)) {
                FintModelObject fintModelObject = reflectionService.initializeFintModelObject(metaSubType);
                addResourceInformation(createFintResourceInformation(
                        metaSubType.getSimpleName().toLowerCase(),
                        metaSubType.getName(),
                        fintModelObject)
                );
                fintModelObject.getRelations().forEach(this::createFintResourceInformationForRelatedRelations);
            }
        });
    }

    private void createFintResourceInformationForRelatedRelations(FintRelation relation) {
        String[] packageSplit = relation.getPackageName().split("\\.");
        if (isAPartOfCommonLibrary(packageSplit)) {

            FintModelObject fintModelObject = reflectionService.initializeFintModelObject(
                    reflectionService.getPackageMetaSubTypeMap().get(relation.getPackageName())
            );

            addResourceInformation(
                    createFintResourceInformation(
                            relation.getName().toLowerCase(),
                            relation.getPackageName(),
                            fintModelObject
                    )
            );
        }
    }

    private boolean isAPartOfCommonLibrary(String[] packageSplit) {
        return packageSplit.length == 5;
    }

    private void fillRelationInformationMap() {
        reflectionService.getPackageMetaSubTypeMap().values().forEach(metaSubType -> {
            if (validRelation(metaSubType)) {
                FintModelObject fintModelObject = reflectionService.initializeFintModelObject(metaSubType);
                fintModelObject.getRelations().forEach(fintRelation -> {
                    String packageName = fintRelation.getPackageName();

                    if (!abstractPackages.contains(packageName) && !referencePackages.contains(packageName)) {
                        Class<? extends FintModelObject> relationMetaSubType = reflectionService.getPackageMetaSubTypeMap().get(packageName);
                        FintModelObject relationFintModelObject = reflectionService.initializeFintModelObject(relationMetaSubType);
                        addRelationInformation(packageName, new FintRelationInformation(relationFintModelObject.getIdentifikators().keySet()));
                    }
                });
            }
        });
    }

    private boolean validRelation(Class<? extends FintModelObject> metaSubType) {
        return metaSubTypeBelongsToThisComponent(metaSubType) || isAPartOfCommonLibrary(metaSubType.getName().split("\\."));
    }

    private FintResourceInformation createFintResourceInformation(String resourceName, String packageName, FintModelObject fintModelObject) {
        return FintResourceInformation.byMetaData(
                resourceName,
                reflectionService.getPackageResourceSubTypeMap().get(packageName),
                fintModelObject
        );
    }

    private boolean metaSubTypeBelongsToThisComponent(Class<? extends FintModelObject> metaSubType) {
        return metaSubType.getPackageName().toLowerCase().contains(
                "%s.%s".formatted(configuration.getDomain(), configuration.getPackageName()).toLowerCase()
        );
    }

    private void addResourceInformation(FintResourceInformation fintResourceInformation) {
        resourceToResourceInformationMap.put(fintResourceInformation.name(), fintResourceInformation);
    }

    private void addRelationInformation(String packageName, FintRelationInformation fintRelationInformation) {
        packageToRelationInformationMap.put(packageName.toLowerCase(), fintRelationInformation);
    }

    private void crashIfNoResourcesAreFound() {
        if (resourceToResourceInformationMap.isEmpty()) {
            throw new RuntimeException("Required subtypes was not found in Fint packages");
        }
    }

}
