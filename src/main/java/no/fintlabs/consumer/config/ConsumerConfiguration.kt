package no.fintlabs.consumer.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Data
@Configuration
public class ConsumerConfiguration {

    public ConsumerConfiguration(ObjectMapper objectMapper) {
        objectMapper.setDateFormat(new ISO8601DateFormat()).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Value("${fint.relation.base-url}")
    private String baseUrl;

    @Value("${fint.consumer.domain}")
    private String domain;

    @Value("${fint.consumer.package}")
    private String packageName;

    @Value("${fint.consumer.org-id}")
    private String orgId;

    @Value("${fint.consumer.pod-url}")
    private String podUrl;

    private String id = UUID.randomUUID().toString();

    public String getComponentUrl() {
        return "%s/%s/%s".formatted(baseUrl, domain, packageName);
    }

    public boolean matchesConfiguration(String domain, String packageName, String orgId) {
        return this.domain.equalsIgnoreCase(domain)
                && this.packageName.equalsIgnoreCase(packageName)
                && this.orgId.equalsIgnoreCase(formatOrgId(orgId));
    }

    private String formatOrgId(String orgId) {
        return orgId.replace("-", ".").replace("_", ".");
    }

}
