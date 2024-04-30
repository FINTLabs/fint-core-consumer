package no.fintlabs.consumer.kafka;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class EntityKafkaConsumerInitializer {

    private final ReflectionService reflectionService;
    private final GenericApplicationContext applicationContext;

    @PostConstruct
    public void init() {
        reflectionService.getResources().forEach((k, v) -> {
            Object bean = applicationContext.getBean(k + "EntityKafkaConsumer");
            log.info("created bean: {}", bean.getClass().getSimpleName());
        });
    }

}
