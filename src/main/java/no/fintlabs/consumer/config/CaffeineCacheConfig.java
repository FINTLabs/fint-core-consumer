package no.fintlabs.consumer.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import no.fintlabs.adapter.models.event.RequestFintEvent;
import no.fintlabs.adapter.models.event.ResponseFintEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class CaffeineCacheConfig {

    @Bean
    public Cache<String, String> stringCache() {
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
