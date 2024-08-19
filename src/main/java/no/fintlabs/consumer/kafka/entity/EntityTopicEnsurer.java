package no.fintlabs.consumer.kafka.entity;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import no.fintlabs.kafka.entity.topic.EntityTopicService;
import no.fintlabs.reflection.ResourceContext;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EntityTopicEnsurer {

    private final EntityTopicService entityTopicService;
    private final ResourceContext resourceContext;
    private final ConsumerConfiguration configuration;
    private final static Long DAY_IN_MILLISECONDS = 86400000L;

    public EntityTopicEnsurer(EntityTopicService entityTopicService, ResourceContext resourceContext, ConsumerConfiguration configuration) {
        this.entityTopicService = entityTopicService;
        this.resourceContext = resourceContext;
        this.configuration = configuration;
        ensureEntityTopics();
    }

    private void ensureEntityTopics() {
        resourceContext.getResourceNames().forEach(resourceName -> {
            entityTopicService.ensureTopic(
                    EntityTopicNameParameters.builder()
                            .orgId("fintlabs-no")
                            .domainContext("fint-core")
                            .resource("%s-%s-%s".formatted(
                                    configuration.getDomain(),
                                    configuration.getPackageName(),
                                    resourceName
                            ))
                            .build(),
                    DAY_IN_MILLISECONDS
            );
        });
    }

}
