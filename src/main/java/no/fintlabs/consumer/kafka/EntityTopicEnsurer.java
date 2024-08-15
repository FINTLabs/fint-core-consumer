package no.fintlabs.consumer.kafka;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import no.fintlabs.kafka.entity.topic.EntityTopicService;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EntityTopicEnsurer {

    private final EntityTopicService entityTopicService;
    private final ReflectionService reflectionService;
    private final static Long DAY_IN_MILLISECONDS = 86400000L;

    public EntityTopicEnsurer(EntityTopicService entityTopicService, ReflectionService reflectionService) {
        this.entityTopicService = entityTopicService;
        this.reflectionService = reflectionService;
        ensureEntityTopics();
    }

    private void ensureEntityTopics() {
        reflectionService.getResources().keySet().forEach(entityName -> {
            log.info("Creating Kafka entity topic for entity: {}", entityName);
            entityTopicService.ensureTopic(
                    EntityTopicNameParameters.builder()
                            .orgId("fintlabs-no")
                            .domainContext("fint-core")
                            .resource(entityName)
                            .build(),
                    DAY_IN_MILLISECONDS
            );
        });
    }

}
