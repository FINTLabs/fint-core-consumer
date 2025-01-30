package no.fintlabs.consumer.kafka.entity;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.cache.CacheService;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.offset.OffsetService;
import no.fintlabs.consumer.resource.ResourceMapper;
import no.fintlabs.consumer.resource.ResourceService;
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern;
import no.fintlabs.kafka.entity.EntityConsumerFactoryService;
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class EntityConsumer {

    private final ResourceService resourceService;
    private final ResourceMapper resourceMapper;
    private final EntityLoggingService entityLoggingService;
    private final CacheService cacheService;
    private final OffsetService offsetService;

    @Bean
    public ConcurrentMessageListenerContainer<String, Object> concurrentMessageListenerContainer(EntityConsumerFactoryService entityConsumerFactoryService,
                                                                                                 ConsumerConfiguration configuration) {
        return entityConsumerFactoryService
                .createFactory(Object.class, this::consumeRecord)
                .createContainer(
                        EntityTopicNamePatternParameters.builder()
                                .orgId(FormattedTopicComponentPattern.anyOf(configuration.getOrgId().replace(".", "-")))
                                .domainContext(FormattedTopicComponentPattern.anyOf("fint-core"))
                                .resource(FormattedTopicComponentPattern.startingWith("%s.%s".formatted(configuration.getDomain(), configuration.getPackageName())))
                                .build()
                );
    }

    private void consumeRecord(ConsumerRecord<String, Object> consumerRecord) {
        String resourceName = getResourceNameFromTopic(consumerRecord.topic());

        entityLoggingService.startLogging(resourceName);
        cacheService.updateRetentionTime(resourceName, consumerRecord.headers().lastHeader("topic-retension-time"));
        offsetService.updateEntityOffset(resourceName, consumerRecord.offset());
        resourceService.addResourceToCache(
                resourceName,
                consumerRecord.key(),
                resourceMapper.mapResource(resourceName, consumerRecord.value())
        );
    }

    private String getResourceNameFromTopic(String topic) {
        String[] split = topic.split("-");
        return split[split.length - 1];
    }

}
