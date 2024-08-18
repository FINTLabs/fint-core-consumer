package no.fintlabs.reflection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintModelObject;
import no.fint.model.resource.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
@Getter
public class ReflectionService {

    private final Set<Class<? extends FintModelObject>> metaSubTypes;
    private final Map<String, Class<? extends FintResource>> resourceSubTypesMap;

    public ReflectionService(ConsumerConfiguration consumerConfig) {
        this.metaSubTypes = new Reflections(String.format("no.fint.model.%s.%s", consumerConfig.getDomain(), consumerConfig.getPackageName())).getSubTypesOf(FintModelObject.class);
        this.resourceSubTypesMap = getResourceSubTypesMap(consumerConfig);
        crashIfNoSubtypesFound();
    }

    public FintModelObject initializeFintModelObject(Class<? extends FintModelObject> metaSubType) {
        try {
            return metaSubType.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Initialization failed when creating a new instance of FintModelObject for: %s".formatted(metaSubType.getSimpleName()));
        }
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

    private void crashIfNoSubtypesFound() {
        if (metaSubTypes.isEmpty() || resourceSubTypesMap.isEmpty()) {
            throw new RuntimeException("Required subtypes was not found in Fint packages");
        }
    }

}
