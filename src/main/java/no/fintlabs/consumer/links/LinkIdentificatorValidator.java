package no.fintlabs.consumer.links;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.reflection.FintResourceInformation;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
@Slf4j
public class LinkIdentificatorValidator {

    private final Map<String, Map<String, Set<String>>> resourceLinkIdMap = new HashMap<>();

    public LinkIdentificatorValidator(ReflectionService reflectionService) {
        setresourceLinkIds(reflectionService);
    }


    // Forgive me, men jeg vil ikke lagre enda enn map med key som pakke navn som refererer til FintResourceInformation
    private void setresourceLinkIds(ReflectionService reflectionService) {
        reflectionService.getResources().forEach((resource, resourceInformation) -> {
            resourceInformation.relations().forEach(fintRelation -> {
                resourceLinkIdMap.put(resource, Map.of(
                                fintRelation.getName(),
                                reflectionService.getResources().values().stream()
                                        .filter(otherResource -> otherResource.clazz().getSimpleName().equals(fintRelation.getPackageName()))
                                        .findFirst()
                                        .map(FintResourceInformation::idFieldNames).get()
                        )
                );
            });
        });
    }

}
