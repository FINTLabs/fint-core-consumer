package no.fintlabs.reflection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintMetaObject;
import no.fint.model.FintResource;
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
    private final Map<String, FintResourceObject> resources = new HashMap<>();
    private final Set<Class<? extends FintMetaObject>> metaSubTypes;
    private final Map<String, Class<? extends FintResource>> resourceSubTypesMap;

    public ReflectionService(ConsumerConfiguration consumerConfig) {
        this.metaSubTypes = new Reflections(String.format("no.fint.model.%s.%s", consumerConfig.getDomain(), consumerConfig.getPackageName())).getSubTypesOf(FintMetaObject.class);
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
                resources.put(
                        metaSubType.getSimpleName().toLowerCase(),
                        FintResourceObject.builder()
                                .clazz(resourceClass)
                                .idFieldNames(getIdentificatorsOfSubType(metaSubType))
                                .build()
                );
                log.debug("Created FintResourceObject for {} with resource class {}", metaSubType.getSimpleName(), resourceClass.getSimpleName());
            } else {
                log.warn("No resource class found for {}", metaSubType.getSimpleName());
            }
        });
    }

    private Set<String> getIdentificatorsOfSubType(Class<? extends FintMetaObject> subType) {
        try {
            FintMetaObject fintMetaObject = subType.getDeclaredConstructor().newInstance();
            return fintMetaObject.getIdentifikators().keySet().stream().map(String::toLowerCase).collect(Collectors.toSet());
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
