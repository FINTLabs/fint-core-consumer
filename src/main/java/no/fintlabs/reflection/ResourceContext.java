package no.fintlabs.reflection;

import lombok.Getter;
import no.fint.model.FintModelObject;
import no.fint.model.resource.FintResource;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Configuration
@Getter
public class ResourceContext {

    private final Set<String> resourceNames = new HashSet<>();
    private final Map<String, FintResourceInformation> fintResourceInformationMap = new HashMap<>();

    public ResourceContext(ReflectionService reflectionService) {
        fillResourceNames(reflectionService);
        fillResourceInformationMap(reflectionService);
    }

    private void fillResourceNames(ReflectionService reflectionService) {
        reflectionService.getMetaSubTypes().forEach(metaSubType -> resourceNames.add(metaSubType.getSimpleName().toLowerCase()));
    }

    private void fillResourceInformationMap(ReflectionService reflectionService) {
        reflectionService.getMetaSubTypes().forEach(metaSubType -> {
            Class<? extends FintResource> resourceClass = reflectionService.getResourceSubTypesMap().get("%sResource".formatted(metaSubType.getSimpleName()));
            if (resourceClass != null) {
                FintModelObject fintModelObject = reflectionService.initializeFintModelObject(metaSubType);
                FintResourceInformation fintResourceInformation = FintResourceInformation.byMetaData(resourceClass, fintModelObject);

                fintResourceInformationMap.put(metaSubType.getSimpleName().toLowerCase(), fintResourceInformation);
                fintResourceInformationMap.put(metaSubType.getName(), fintResourceInformation);
            } else {
                throw new RuntimeException("No resourceClass was found");
            }
        });
    }

}
