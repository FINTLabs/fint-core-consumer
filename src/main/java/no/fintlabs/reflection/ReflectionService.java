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
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
@Getter
public class ReflectionService {

    private final Map<String, Class<? extends FintModelObject>> allMetaSubTypesMap;
    private final Set<Class<? extends FintModelObject>> componentMetaSubTypes;
    private final Map<String, Class<? extends FintResource>> componentResourceSubTypesMap;

    public ReflectionService(ConsumerConfiguration consumerConfig) {
        this.allMetaSubTypesMap = setAllMetaSubTypesMap();
        this.componentMetaSubTypes = new Reflections(String.format("no.fint.model.%s.%s", consumerConfig.getDomain(), consumerConfig.getPackageName())).getSubTypesOf(FintModelObject.class);
        this.componentResourceSubTypesMap = setComponentResourceSubTypesMap(consumerConfig);
        crashIfNoSubtypesFound();
    }

    public Class<? extends FintResource> getResourceSubType(String resourceName) {
        return componentResourceSubTypesMap.get(resourceName);
    }

    private Map<String, Class<? extends FintModelObject>> setAllMetaSubTypesMap() {
        return new Reflections("no.fint.model").getSubTypesOf(FintModelObject.class).stream()
                .collect(Collectors.toMap(
                                Class::getName,
                                Function.identity()
                        )
                );
    }

    public FintModelObject initializeFintModelObject(Class<? extends FintModelObject> metaSubType) {
        try {
            return metaSubType.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Initialization failed when creating a new instance of FintModelObject for: %s".formatted(metaSubType.getSimpleName()));
        }
    }

    private Map<String, Class<? extends FintResource>> setComponentResourceSubTypesMap(ConsumerConfiguration configuration) {
        return new Reflections(String.format("no.fint.model.resource.%s.%s", configuration.getDomain(), configuration.getPackageName()))
                .getSubTypesOf(FintResource.class)
                .stream()
                .collect(Collectors.toMap(
                        clazz -> clazz.getSimpleName().replace("Resource", ""),
                        clazz -> clazz
                ));
    }

    private void crashIfNoSubtypesFound() {
        if (componentMetaSubTypes.isEmpty() || componentResourceSubTypesMap.isEmpty()) {
            throw new RuntimeException("Required subtypes was not found in Fint packages");
        }
    }

}
