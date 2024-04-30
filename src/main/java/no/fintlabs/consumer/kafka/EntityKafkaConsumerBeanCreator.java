package no.fintlabs.consumer.kafka;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.kafka.entity.EntityConsumerFactoryService;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;

@Configuration
@ComponentScan("no.fintlabs.kafka")
@Slf4j
public class EntityKafkaConsumerBeanCreator {

    private final ReflectionService reflectionService;
    private final EntityConsumerFactoryService entityConsumerFactoryService;
    private final ConsumerConfiguration configuration;
    private final GenericApplicationContext applicationContext;

    public EntityKafkaConsumerBeanCreator(ReflectionService reflectionService, EntityConsumerFactoryService entityConsumerFactoryService, ConsumerConfiguration configuration, GenericApplicationContext applicationContext) {
        this.reflectionService = reflectionService;
        this.entityConsumerFactoryService = entityConsumerFactoryService;
        this.configuration = configuration;
        this.applicationContext = applicationContext;
        createKafkaConsumers();
    }

    public void createKafkaConsumers() {
        reflectionService.getResources().forEach((resourceName, fintObject) -> {
            log.info("Creating kafka consumer for: {}", resourceName);
            EntityKafkaConsumerFactoryBean factoryBean = new EntityKafkaConsumerFactoryBean();
            factoryBean.setThings(resourceName, configuration, fintObject.getClazz(), entityConsumerFactoryService);
            String beanName = String.format("%sEntityKafkaConsumer", resourceName);
            applicationContext.registerBean(beanName, EntityKafkaConsumerFactoryBean.class, () -> factoryBean);
        });
    }

    private <T extends FintResource> EntityKafkaConsumer<T> helperMethod(String resourceName, Class<T> clazz) {
        return new EntityKafkaConsumer<>(entityConsumerFactoryService, resourceName, configuration.getDomain(), configuration.getPackageName(), clazz);
    }

}
