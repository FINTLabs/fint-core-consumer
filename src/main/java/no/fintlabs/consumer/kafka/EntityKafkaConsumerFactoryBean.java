package no.fintlabs.consumer.kafka;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.kafka.entity.EntityConsumerFactoryService;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

@Slf4j
public class EntityKafkaConsumerFactoryBean implements FactoryBean<EntityKafkaConsumer<? extends FintResource>>, InitializingBean {

    private String resourceName;
    private ConsumerConfiguration configuration;
    private Class<? extends FintResource> clazz;
    private EntityConsumerFactoryService entityConsumerFactoryService;

    public void setThings(String resourceName, ConsumerConfiguration configuration, Class<? extends FintResource> clazz, EntityConsumerFactoryService entityConsumerFactoryService) {
        this.resourceName = resourceName;
        this.configuration = configuration;
        this.clazz = clazz;
        this.entityConsumerFactoryService = entityConsumerFactoryService;
    }

    @Override
    public EntityKafkaConsumer<? extends FintResource> getObject() throws Exception {
        return new EntityKafkaConsumer<>(
                entityConsumerFactoryService,
                configuration.getDomain(),
                configuration.getPackageName(),
                resourceName,
                clazz
        );
    }

    @Override
    public Class<?> getObjectType() {
        return EntityKafkaConsumer.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("done setting properties");
    }

}
