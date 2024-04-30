package no.fintlabs.reflection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintMetaObject;
import no.fint.model.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.*;

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
        createResourceObjects();
    }

    private void createResourceObjects() {
        metaSubTypes.forEach(metaSubType -> {
            Optional<Class<? extends FintResource>> optionalResourceSubType = getOptionalResourceSubType(metaSubType.getSimpleName().toLowerCase());
            optionalResourceSubType.ifPresent(aClass -> resources.put(
                    metaSubType.getSimpleName().toLowerCase(),
                    FintResourceObject.builder()
                            .clazz(aClass)
                            .idFieldNames(getIdentificators(metaSubType))
                            .build()
            ));
        });
    }

    private Set<String> getIdentificators(Class<? extends FintMetaObject> subType) {
        try {
            FintMetaObject fintMetaObject = subType.getDeclaredConstructor().newInstance();
            return fintMetaObject.getIdentifikators().keySet();
        } catch (Exception e) {
        }
        return new HashSet<>();
    }

    private Optional<Class<? extends FintResource>> getOptionalResourceSubType(String resourceName) {
        return resourceSubTypes.stream()
                .filter(s -> s.getSimpleName().toLowerCase().replace("resource", "").equals(resourceName))
                .findFirst();
    }

}
