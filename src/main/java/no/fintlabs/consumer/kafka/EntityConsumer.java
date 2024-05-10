package no.fintlabs.consumer.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.resource.ResourceService;
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern;
import no.fintlabs.kafka.entity.EntityConsumerFactoryService;
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Slf4j
@Configuration
@RequiredArgsConstructor
@ComponentScan("no.fintlabs.kafka")
public class EntityConsumer {

    private final ResourceService resourceService;

    @Bean
    public ConcurrentMessageListenerContainer<String, String> concurrentMessageListenerContainer(EntityConsumerFactoryService entityConsumerFactoryService,
                                                                                                 ConsumerConfiguration configuration) {
        return entityConsumerFactoryService
                .createFactory(String.class, this::consumeRecord)
                .createContainer(
                        EntityTopicNamePatternParameters.builder()
                                .resource(FormattedTopicComponentPattern.startingWith("%s.%s".formatted(configuration.getDomain(), configuration.getPackageName())))
                                .build()
                );
    }

    private void consumeRecord(ConsumerRecord<String, String> consumerRecord) {
        log.info("Consumed: {}", consumerRecord.value());
        String[] split = consumerRecord.topic().split("-");

        String resourceName = split[split.length - 1];
        String resourceData = consumerRecord.value();
        FintResource fintResource = resourceService.mapResource(resourceName, resourceData);

        resourceService.addResourceToCache(resourceName, consumerRecord.key(), fintResource);
    }

}
