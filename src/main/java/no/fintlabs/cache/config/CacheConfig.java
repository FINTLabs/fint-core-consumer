package no.fintlabs.cache.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("fint.consumer.cache")
public class CacheConfig {

    private Long retention = 604800000L;
    private String evictionCron = "0 0 * * * ?";

}
