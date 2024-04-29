package no.fintlabs.reflection;

import lombok.Getter;
import no.fint.model.FintMetaObject;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Service
public class ReflectionService {

    private final Map<String, Set<String>> resources;
    private final Set<Class<? extends FintMetaObject>> subTypes;

    public ReflectionService(ConsumerConfiguration consumerConfig) {
        this.subTypes = new Reflections(String.format("no.fint.model.%s.%s", consumerConfig.getDomain(), consumerConfig.getPackageName())).getSubTypesOf(FintMetaObject.class);
        this.resources = setResources();
    }

    private Map<String, Set<String>> setResources() {
        Map<String, Set<String>> resources = new HashMap<>();

        subTypes.forEach(subType -> {
            try {
                var fintMetaObject = subType.getDeclaredConstructor().newInstance();
                resources.computeIfAbsent(subType.getSimpleName().toLowerCase(), k -> new HashSet<>())
                        .addAll(fintMetaObject.getIdentifikators().keySet().stream()
                                .map(String::toLowerCase) // Convert each key to lowercase
                                .collect(Collectors.toSet()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return resources;
    }
}
