package no.fintlabs.cache;

import lombok.RequiredArgsConstructor;
import no.fintlabs.kafka.event.EventConsumerFactoryService;
import no.fintlabs.kafka.event.topic.EventTopicNameParameters;
import no.fintlabs.status.models.ResourceEvictionPayload;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CompletedFullSyncConsumer {

    private final CacheEvictionService cacheEvictionService;
    private final EventTopicNameParameters eventTopicName = createEventTopicNameParameters();

    @Bean
    public ConcurrentMessageListenerContainer<String, ResourceEvictionPayload> completedFullSyncListener(EventConsumerFactoryService eventConsumerFactoryService) {
        return eventConsumerFactoryService.createFactory(ResourceEvictionPayload.class, this::consume)
                .createContainer(eventTopicName);
    }

    private void consume(ConsumerRecord<String, ResourceEvictionPayload> consumerRecord) {
        cacheEvictionService.triggerEviction(consumerRecord.value());
    }

    private EventTopicNameParameters createEventTopicNameParameters() {
        return EventTopicNameParameters.builder()
                .orgId("fintlabs-no")
                .domainContext("fint-core")
                .eventName("completed-full-sync")
                .build();
    }

}
