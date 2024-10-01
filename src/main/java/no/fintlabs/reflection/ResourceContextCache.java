package no.fintlabs.reflection;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintModelObject;
import no.fint.model.FintRelation;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class ResourceContextCache {

    private final ReflectionService reflectionService;
    private final ConsumerConfiguration configuration;

    protected final Map<String, FintResourceInformation> resourceToResourceInformationMap = new HashMap<>();
    protected final Map<String, FintRelationInformation> packageToRelationInformationMap = new HashMap<>();

    public ResourceContextCache(ReflectionService reflectionService, ConsumerConfiguration configuration) {
        this.reflectionService = reflectionService;
        this.configuration = configuration;
        fillResourceInformationMap();
        fillRelationInformationMap();
    }

    private FintResourceInformation createFintResourceInformation(String resourceName, String packageName, FintModelObject fintModelObject) {
        return FintResourceInformation.byMetaData(
                resourceName,
                reflectionService.getPackageResourceSubTypeMap().get(packageName),
                fintModelObject
        );
    }

    private void fillResourceInformationMap() {
        reflectionService.getPackageMetaSubTypeMap().values().forEach(metaSubType -> {
            if (metaSubTypeBelongsToThisComponent(metaSubType, configuration)) {
                FintModelObject fintModelObject = reflectionService.initializeFintModelObject(metaSubType);
                addResourceInformation(createFintResourceInformation(metaSubType.getSimpleName().toLowerCase(), metaSubType.getName(), fintModelObject));
                fintModelObject.getRelations().forEach(this::createFintResourceInformationForRelatedRelations);
            }
        });
    }

    private void createFintResourceInformationForRelatedRelations(FintRelation relation) {
        String[] packageSplit = relation.getPackageName().split("\\.");
        if (isAPartOfCommonLibrary(packageSplit)) {
            FintModelObject fintModelObject = reflectionService.initializeFintModelObject(reflectionService.getPackageMetaSubTypeMap().get(relation.getPackageName()));
            addResourceInformation(createFintResourceInformation(packageSplit[packageSplit.length - 1].toLowerCase(), relation.getPackageName(), fintModelObject));
        }
    }

    private boolean isAPartOfCommonLibrary(String[] packageSplit) {
        return packageSplit.length == 5;
    }

    private void fillRelationInformationMap() {
        reflectionService.getPackageMetaSubTypeMap().values().forEach(metaSubType -> {
            if (metaSubTypeBelongsToThisComponent(metaSubType, configuration) || isAPartOfCommonLibrary(metaSubType.getName().split("\\."))) {
                FintModelObject fintModelObject = reflectionService.initializeFintModelObject(metaSubType);
                fintModelObject.getRelations().forEach(fintRelation -> {
                    FintModelObject relationFintModelObject = reflectionService.initializeFintModelObject(reflectionService.getPackageMetaSubTypeMap().get(fintRelation.getPackageName()));
                    addRelationInformation(fintRelation.getPackageName(), new FintRelationInformation(relationFintModelObject.getIdentifikators().keySet()));
                });
            }
        });
    }

    private boolean metaSubTypeBelongsToThisComponent(Class<? extends FintModelObject> metaSubType, ConsumerConfiguration configuration) {
        return metaSubType.getPackageName().toLowerCase().contains("%s.%s".formatted(configuration.getDomain(), configuration.getPackageName()).toLowerCase());
    }

    private void addResourceInformation(FintResourceInformation fintResourceInformation) {
        resourceToResourceInformationMap.put(fintResourceInformation.name(), fintResourceInformation);
    }

    private void addRelationInformation(String packageName, FintRelationInformation fintRelationInformation) {
        packageToRelationInformationMap.put(packageName.toLowerCase(), fintRelationInformation);
    }

}
