package no.fintlabs.reflection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintModelObject;
import no.fint.model.resource.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ReflectionService {

    @Getter
    private final Map<String, FintResourceInformation> resources = new HashMap<>();
    private final Set<Class<? extends FintModelObject>> metaSubTypes;
    private final Map<String, Class<? extends FintResource>> resourceSubTypesMap;

    public ReflectionService(ConsumerConfiguration consumerConfig) {
        this.metaSubTypes = new Reflections(String.format("no.fint.model.%s.%s", consumerConfig.getDomain(), consumerConfig.getPackageName())).getSubTypesOf(FintModelObject.class);
        this.resourceSubTypesMap = getResourceSubTypesMap(consumerConfig);
        crashIfNoSubtypesFound();
        createResourceObjects();
    }

    private Map<String, Class<? extends FintResource>> getResourceSubTypesMap(ConsumerConfiguration configuration) {
        return new Reflections(String.format("no.fint.model.resource.%s.%s", configuration.getDomain(), configuration.getPackageName()))
                .getSubTypesOf(FintResource.class)
                .stream()
                .collect(Collectors.toMap(
                        Class::getSimpleName,
                        clazz -> clazz
                ));
    }

    private void createResourceObjects() {
        metaSubTypes.forEach(metaSubType -> {
            var resourceClass = resourceSubTypesMap.get(metaSubType.getSimpleName() + "Resource");
            if (resourceClass != null) {
                try {
                    FintModelObject fintModelObject = metaSubType.getDeclaredConstructor().newInstance();
                    resources.put(
                            metaSubType.getSimpleName().toLowerCase(),
                            new FintResourceInformation(resourceClass, getIdentificatorsOfSubType(fintModelObject), getRelationLinks(fintModelObject))
                    );
                    log.debug("Created FintResourceObjectObject for {} with resource class {}", metaSubType.getSimpleName(), resourceClass.getSimpleName());
                } catch (Exception e) {
                    log.error("Error while getting identifiers for subtype: {}", metaSubType.getSimpleName(), e);
                }
            } else {
                log.warn("No resource class found for {}", metaSubType.getSimpleName());
            }
        });
    }

    private Map<String, String> getRelationLinks(FintModelObject fintModelObject) {
        return fintModelObject.getRelations().stream().collect(Collectors.toMap(
                fintRelation -> fintRelation.getName().toLowerCase(),
                fintRelation -> fintRelation.getPackageName().replaceFirst("no.fint.model", "").replace(".", "/").toLowerCase()));
    }

    private Set<String> getIdentificatorsOfSubType(FintModelObject fintModelObject) {
        return fintModelObject.getIdentifikators().keySet().stream().map(String::toLowerCase).collect(Collectors.toSet());
    }

    private void crashIfNoSubtypesFound() {
        if (metaSubTypes.isEmpty() || resourceSubTypesMap.isEmpty()) {
            throw new RuntimeException("No subtypes found in Fint packages");
        }
    }

}
