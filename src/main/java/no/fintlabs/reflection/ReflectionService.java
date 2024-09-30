package no.fintlabs.reflection;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintModelObject;
import no.fint.model.resource.FintResource;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Getter
@Service
public class ReflectionService {

    private final Map<String, Class<? extends FintModelObject>> packageMetaSubTypeMap = createPackageMetaSubTypeMap();
    private final Map<String, Class<? extends FintResource>> packageResourceSubTypeMap = createPackageResourceSubTypeMap();

    private Map<String, Class<? extends FintModelObject>> createPackageMetaSubTypeMap() {
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

    private Map<String, Class<? extends FintResource>> createPackageResourceSubTypeMap() {
        return new Reflections("no.fint.model.resource")
                .getSubTypesOf(FintResource.class)
                .stream()
                .collect(Collectors.toMap(
                        clazz -> clazz.getName().replace("Resource", ""),
                        clazz -> clazz
                ));
    }

    @PostConstruct
    private void crashIfNoSubtypesFound() {
        if (packageMetaSubTypeMap.isEmpty() || packageResourceSubTypeMap.isEmpty()) {
            throw new RuntimeException("Required subtypes was not found in Fint packages");
        }
    }

}
