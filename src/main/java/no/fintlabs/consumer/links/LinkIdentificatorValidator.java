package no.fintlabs.consumer.links;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
@Slf4j
public class LinkIdentificatorValidator {

    private final Map<String, Map<String, Set<String>>> resourceLinkIdMap = new HashMap<>();

    public LinkIdentificatorValidator(ResourceContext resourceContext) {
        setresourceLinkIds(resourceContext);
    }

    private void setresourceLinkIds(ResourceContext resourceContext) {
        resourceContext.getFintResourceInformationMap().forEach((resourceName, resourceInformation) -> {
            resourceInformation.relations().forEach(fintRelation ->
                    resourceLinkIdMap.put(resourceName, Map.of(
                                    fintRelation.getName(),
                                    resourceContext.getFintResourceInformationMap().get(fintRelation.getPackageName()).idFieldNames()
                            )
                    )
            );
        });
    }

}
