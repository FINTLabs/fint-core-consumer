package no.fintlabs.reflection;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintAbstractObject;
import no.fint.model.FintModelObject;
import no.fint.model.FintReference;
import no.fint.model.resource.FintResource;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Getter
@Service
public class ReflectionService {

    private final Map<String, Class<? extends FintModelObject>> packageMetaSubTypeMap;
    private final Map<String, Class<? extends FintResource>> packageResourceSubTypeMap;
    private final List<Class<? extends FintAbstractObject>> fintAbstractSubTypes;
    private final List<Class<? extends FintReference>> fintReferenceSubTypes;

    public ReflectionService() {
        Reflections reflections = new Reflections("no.fint.model");
        packageMetaSubTypeMap = createPackageMetaSubTypeMap(reflections);
        packageResourceSubTypeMap = createPackageResourceSubTypeMap(reflections);
        fintAbstractSubTypes = createAbstractPackageNames(reflections);
        fintReferenceSubTypes = createReferencePackageNames(reflections);
    }

    private List<Class<? extends FintReference>> createReferencePackageNames(Reflections reflections) {
        return new ArrayList<>(reflections.getSubTypesOf(FintReference.class));
    }

    private List<Class<? extends FintAbstractObject>> createAbstractPackageNames(Reflections reflections) {
        return reflections.getSubTypesOf(FintAbstractObject.class).stream()
                .filter(clazz -> !packageMetaSubTypeMap.containsKey(clazz.getName()))
                .collect(Collectors.toList());
    }

    private Map<String, Class<? extends FintModelObject>> createPackageMetaSubTypeMap(Reflections reflections) {
        return reflections.getSubTypesOf(FintModelObject.class).stream()
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

    private Map<String, Class<? extends FintResource>> createPackageResourceSubTypeMap(Reflections reflections) {
        return reflections
                .getSubTypesOf(FintResource.class)
                .stream()
                .collect(Collectors.toMap(
                        clazz -> clazz.getName().replace("Resource", "").replace(".resource", ""),
                        clazz -> clazz
                ));
    }

}
