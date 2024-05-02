package no.fintlabs.consumer.kafka;

import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.kafka.entity.EntityConsumerFactoryService;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@ComponentScan("no.fintlabs.kafka")
public class EntityKafkaBeanCreator {

    private final GenericApplicationContext applicationContext;
    private final ReflectionService reflectionService;
    private final EntityConsumerFactoryService entityConsumerFactoryService;
    private final ConsumerConfiguration configuration;

    public EntityKafkaBeanCreator(GenericApplicationContext applicationContext, ReflectionService reflectionService, EntityConsumerFactoryService entityConsumerFactoryService, ConsumerConfiguration configuration) {
        this.applicationContext = applicationContext;
        this.reflectionService = reflectionService;
        this.entityConsumerFactoryService = entityConsumerFactoryService;
        this.configuration = configuration;

        registerBeans();
    }

    public void registerBeans() {
        reflectionService.getResources().forEach((resourceName, fintResourceObject) -> {
            EntityKafkaFactoryBean entityKafkaFactoryBean = new EntityKafkaFactoryBean(
                    entityConsumerFactoryService,
                    configuration,
                    resourceName,
                    fintResourceObject.getClazz()
            );
            String beanName = resourceName + "EntityKafkaConsumer";
            log.info("Registering bean for: {}", beanName);
            applicationContext.registerBean(beanName, EntityKafkaFactoryBean.class, () -> entityKafkaFactoryBean);
        });
    }

}
