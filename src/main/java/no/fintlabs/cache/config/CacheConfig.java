package no.fintlabs.cache.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class CacheConfig {

    @Value("${fint.consumer.cache.default-retention:604800000}")
    private Long retention;

}
