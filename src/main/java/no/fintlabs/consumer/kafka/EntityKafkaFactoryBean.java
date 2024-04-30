package no.fintlabs.consumer.kafka;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.kafka.entity.EntityConsumerFactoryService;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

@Slf4j
@AllArgsConstructor
public class EntityKafkaFactoryBean implements FactoryBean<EntityKafkaConsumer<? extends FintResource>>, InitializingBean {

    private final EntityConsumerFactoryService entityConsumerFactoryService;
    private final ConsumerConfiguration configuration;
    private final String resourceName;
    private final Class<? extends FintResource> clazz;

    @Override
    public EntityKafkaConsumer<? extends FintResource> getObject() throws Exception {
        EntityKafkaConsumer<? extends FintResource> fintResourceEntityKafkaConsumer = new EntityKafkaConsumer<>(
                entityConsumerFactoryService,
                configuration.getDomain(),
                configuration.getPackageName(),
                resourceName,
                clazz
        );
        log.info("Created EntityKafka consumer for: {}", resourceName);
        return fintResourceEntityKafkaConsumer;
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
        log.info("Done setting properties for: {}", resourceName);
    }
}
