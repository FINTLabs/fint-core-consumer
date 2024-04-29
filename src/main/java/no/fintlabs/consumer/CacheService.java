package no.fintlabs.consumer;

import lombok.RequiredArgsConstructor;
import no.fintlabs.cache.CacheContainer;
import no.fintlabs.reflection.ReflectionService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class CacheService {

    private final ReflectionService reflectionService;

    @Bean
    public CacheContainer cacheContainer() {
        CacheContainer cacheContainer = new CacheContainer();

        reflectionService.getResources().forEach((resource, idFields) -> {
            idFields.forEach(idField -> cacheContainer.initializeCache(resource, idField.toLowerCase()));
        });

        return cacheContainer;
    }

}
