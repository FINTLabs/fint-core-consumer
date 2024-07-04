package no.fintlabs.consumer.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class ConsumerConfiguration {

    @Value("${fint.relation.base-url}")
    private String baseUrl;

    @Value("${fint.consumer.domain}")
    private String domain;

    @Value("${fint.consumer.package}")
    private String packageName;

    @Value("${fint.consumer.org-id}")
    private String orgId;

    public String getComponentUrl() {
        return "%s/%s/%s".formatted(baseUrl, domain, packageName);
    }

}
