package no.fintlabs.consumer.kafka;

import lombok.extern.slf4j.Slf4j;
import no.fint.model.FintResource;
import no.fintlabs.kafka.entity.EntityConsumerFactoryService;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
public class EntityKafkaConsumer<T extends FintResource> {

    private final String resourceName;

    public EntityKafkaConsumer(EntityConsumerFactoryService entityConsumerFactoryService, String domain, String packageName, String resourceName, Class<T> clazz) {
        EntityTopicNameParameters entityTopicNameParameters = EntityTopicNameParameters
                .builder()
                .orgId("fintlabs-no")
                .domainContext("fint-core")
                .resource(String.format("%s.%s.%s", domain, packageName, resourceName))
                .build();

        this.resourceName = resourceName;

        entityConsumerFactoryService
                .createFactory(clazz, this::consumeRecord)
                .createContainer(entityTopicNameParameters);
    }

    private void consumeRecord(ConsumerRecord<String, T> consumerRecord) {
        log.info(consumerRecord.value().toString());
    }

}
