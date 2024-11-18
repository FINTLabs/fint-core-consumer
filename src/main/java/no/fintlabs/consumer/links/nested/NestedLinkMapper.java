package no.fintlabs.consumer.links.nested;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class NestedLinkMapper {

    private final ReflectionService reflectionService;
    private final ConsumerConfiguration configuration;

    @Getter
    private final Map<String, String> packageToUriMap;

    public NestedLinkMapper(ReflectionService reflectionService, ConsumerConfiguration configuration) {
        this.reflectionService = reflectionService;
        this.configuration = configuration;
        this.packageToUriMap = createPackageToUriMap();
    }

    private Map<String, String> createPackageToUriMap() {
        return reflectionService.getPackageMetaSubTypeMap().values().stream()
                .collect(Collectors.toMap(
                        s -> formatPackageName(s.getName()),
                        s -> createUriFromFormattedPackageName(formatPackageName(s.getName()))
                ));
    }

    private String createUriFromFormattedPackageName(String formattedPackageName) {
        String[] split = formattedPackageName.split("\\.");

        if (split.length == 2) {
            return "%s/%s/%s".formatted(
                    configuration.getDomain(),
                    configuration.getPackageName(),
                    split[split.length - 1]
            );
        }

        return formattedPackageName.replace(".", "/");
    }

    private String formatPackageName(String packageName) {
        return packageName
                .replaceFirst("^no\\.fint\\.model(\\.resource)?\\.", "")
                .replaceFirst("Resource$", "")
                .toLowerCase();
    }

}
