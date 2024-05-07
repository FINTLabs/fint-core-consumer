package no.fintlabs.reflection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintMetaObject;
import no.fint.model.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Getter
@Service
@Slf4j
public class ReflectionService {

    private final Map<String, FintResourceObject> resources = new HashMap<>();
    private final Set<Class<? extends FintMetaObject>> metaSubTypes;
    private final Set<Class<? extends FintResource>> resourceSubTypes;

    public ReflectionService(ConsumerConfiguration consumerConfig) {
        this.metaSubTypes = new Reflections(String.format("no.fint.model.%s.%s", consumerConfig.getDomain(), consumerConfig.getPackageName())).getSubTypesOf(FintMetaObject.class);
        this.resourceSubTypes = new Reflections(String.format("no.fint.model.resource.%s.%s", consumerConfig.getDomain(), consumerConfig.getPackageName())).getSubTypesOf(FintResource.class);
        crashIfNoSubtypesFound();
        createResourceObjects();
    }

    private void crashIfNoSubtypesFound() {
        if (metaSubTypes.isEmpty() || resourceSubTypes.isEmpty()) {
            throw new RuntimeException("No subtypes found in Fint packages");
        }
    }

    private void createResourceObjects() {
        metaSubTypes.forEach(metaSubType -> {
            Optional<Class<? extends FintResource>> optionalResourceSubType = getOptionalResourceSubType(metaSubType.getSimpleName().toLowerCase());
            if (optionalResourceSubType.isPresent()) {
                FintResourceObject resourceObject = FintResourceObject.builder()
                        .clazz(optionalResourceSubType.get())
                        .idFieldNames(getIdentificators(metaSubType))
                        .build();
                resources.put(metaSubType.getSimpleName().toLowerCase(), resourceObject);

                log.info("Created FintResourceObject for meta type: {} with resource class: {}", metaSubType.getSimpleName(), optionalResourceSubType.get().getSimpleName());
            } else {
                log.warn("No resource class found for meta type: {}", metaSubType.getSimpleName());
            }
        });
    }

    private Set<String> getIdentificators(Class<? extends FintMetaObject> subType) {
        try {
            FintMetaObject fintMetaObject = subType.getDeclaredConstructor().newInstance();
            return fintMetaObject.getIdentifikators().keySet().stream().map(String::toLowerCase).collect(Collectors.toSet());
        } catch (Exception e) {
            log.error("Error while getting identifiers for subtype: {}", subType.getSimpleName(), e);
        }
        return new HashSet<>();
    }

    private Optional<Class<? extends FintResource>> getOptionalResourceSubType(String resourceName) {
        return resourceSubTypes.stream()
                .filter(s -> s.getSimpleName().toLowerCase().replace("resource", "").equals(resourceName))
                .findFirst();
    }

}
