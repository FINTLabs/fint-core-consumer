package no.fintlabs.reflection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintModelObject;
import no.fint.model.FintResourceObject;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ReflectionService {

    @Getter
    private final Map<String, FintResourceInformation> resources = new HashMap<>();
    private final Set<Class<? extends FintModelObject>> metaSubTypes;
    private final Map<String, Class<? extends FintResourceObject>> resourceSubTypesMap;

    public ReflectionService(ConsumerConfiguration consumerConfig) {
        this.metaSubTypes = new Reflections(String.format("no.fint.model.%s.%s", consumerConfig.getDomain(), consumerConfig.getPackageName())).getSubTypesOf(FintModelObject.class);
        this.resourceSubTypesMap = getResourceSubTypesMap(consumerConfig);
        crashIfNoSubtypesFound();
        createResourceObjects();
    }

    private Map<String, Class<? extends FintResourceObject>> getResourceSubTypesMap(ConsumerConfiguration configuration) {
        return new Reflections(String.format("no.fint.model.resource.%s.%s", configuration.getDomain(), configuration.getPackageName()))
                .getSubTypesOf(FintResourceObject.class)
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
                resources.put(
                        metaSubType.getSimpleName().toLowerCase(),
                        new FintResourceInformation(resourceClass, getIdentificatorsOfSubType(metaSubType))
                );
                log.debug("Created FintResourceObjectObject for {} with resource class {}", metaSubType.getSimpleName(), resourceClass.getSimpleName());
            } else {
                log.warn("No resource class found for {}", metaSubType.getSimpleName());
            }
        });
    }

    private Set<String> getIdentificatorsOfSubType(Class<? extends FintModelObject> subType) {
        try {
            FintModelObject FintModelObject = subType.getDeclaredConstructor().newInstance();
            return FintModelObject.getIdentifikators().keySet().stream().map(String::toLowerCase).collect(Collectors.toSet());
        } catch (Exception e) {
            log.error("Error while getting identifiers for subtype: {}", subType.getSimpleName(), e);
        }
        return new HashSet<>();
    }

    private void crashIfNoSubtypesFound() {
        if (metaSubTypes.isEmpty() || resourceSubTypesMap.isEmpty()) {
            throw new RuntimeException("No subtypes found in Fint packages");
        }
    }

}
