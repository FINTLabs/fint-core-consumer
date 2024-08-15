package no.fintlabs.consumer.kafka;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import no.fintlabs.kafka.entity.topic.EntityTopicService;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EntityTopicEnsurer {

    private final EntityTopicService entityTopicService;
    private final ReflectionService reflectionService;
    private final static Long DAY_IN_MILLISECONDS = 86400000L;

    @PostConstruct
    private void ensureEntityTopics() {
        reflectionService.getResources().keySet().forEach(entityName -> {
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
