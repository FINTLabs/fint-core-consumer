package no.fintlabs.consumer.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import no.fintlabs.adapter.models.RequestFintEvent;
import no.fintlabs.adapter.models.ResponseFintEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class EventCacheConfig {

    @Bean
    public Cache<String, RequestFintEvent> requestFintEvents() {
        return Caffeine.newBuilder()
                .expireAfterWrite(4, TimeUnit.HOURS)
                .build();
    }

    @Bean
    public Cache<String, ResponseFintEvent> responseFintEvents() {
        return Caffeine.newBuilder()
                .expireAfterWrite(4, TimeUnit.HOURS)
                .build();
    }

}
