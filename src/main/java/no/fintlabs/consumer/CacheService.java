package no.fintlabs.consumer;

import lombok.RequiredArgsConstructor;
import no.fintlabs.cache.CacheContainer;
import no.fintlabs.cache.CacheManager;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@ComponentScan("no.fintlabs.cache")
public class CacheService {

    private final ReflectionService reflectionService;

    @Bean
    public CacheContainer cacheContainer(ConsumerConfiguration configuration, CacheManager cacheManager) {
        CacheContainer cacheContainer = new CacheContainer(configuration, cacheManager);

        reflectionService.getResources().forEach((resource, idField) -> cacheContainer.initializeCache(resource.toLowerCase()));

        return cacheContainer;
    }

}
