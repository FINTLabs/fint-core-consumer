package no.fintlabs.reflection;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintAbstractObject;
import no.fint.model.FintModelObject;
import no.fint.model.FintReference;
import no.fint.model.resource.FintResource;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Getter
@Service
public class ReflectionService {

    private final Map<String, Class<? extends FintModelObject>> packageMetaSubTypeMap = createPackageMetaSubTypeMap();
    private final Map<String, Class<? extends FintResource>> packageResourceSubTypeMap = createPackageResourceSubTypeMap();
    private final Set<String> abstractPackageNames = createAbstractPackageNames();
    private final Set<String> referencePackageNames = createReferencePackageNames();
    private final Set<String> referenceSimpleNames = createReferenceSimpleNames();

    public boolean packageIsNotAbstract(String packageName) {
        return !abstractPackageNames.contains(packageName);
    }

    public boolean packageIsNotAReference(String packageName) {
        return !referencePackageNames.contains(packageName);
    }

    public boolean relationNameIsNotAReference(String relationName) {
        return !referenceSimpleNames.contains(relationName.toLowerCase());
    }

    private Set<String> createReferencePackageNames() {
        return new Reflections("no.fint.model").getSubTypesOf(FintReference.class).stream()
                .map(Class::getName)
                .collect(Collectors.toSet());
    }

    private Set<String> createReferenceSimpleNames() {
        return new Reflections("no.fint.model").getSubTypesOf(FintReference.class).stream()
                .map(clazz -> clazz.getSimpleName().toLowerCase())
                .collect(Collectors.toSet());
    }

    private Set<String> createAbstractPackageNames() {
        return new Reflections("no.fint.model").getSubTypesOf(FintAbstractObject.class).stream()
                .map(Class::getName)
                .filter(name -> !packageMetaSubTypeMap.containsKey(name))
                .collect(Collectors.toSet());
    }

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
                        clazz -> clazz.getName().replace("Resource", "").replace(".resource", ""),
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
