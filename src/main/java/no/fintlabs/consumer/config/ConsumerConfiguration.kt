package no.fintlabs.consumer.config

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.ConstructorBinding
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder


@ConfigurationProperties(prefix = "fint.consumer")
data class ConsumerConfiguration @ConstructorBinding constructor(
    val baseUrl: String,
    val orgId: String,
    val domain: String,
    val packageName: String,
    val podUrl: String
) {

    @Bean
    @Primary
    fun jackson2ObjectMapperBuilder(): Jackson2ObjectMapperBuilder {
        return Jackson2ObjectMapperBuilder()
            .failOnUnknownProperties(false)
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            // Register the JavaTimeModule to handle Java 8 Date and Time API types
            .modules(JavaTimeModule())
            // Disable writing dates as timestamps
            .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    }

    val componentUrl: String
        get() = "${baseUrl}/${domain}/${packageName}"

    fun matchesConfiguration(domain: String, packageName: String, orgId: String): Boolean {
        return this.domain.equals(domain, ignoreCase = true)
                && this.packageName.equals(packageName, ignoreCase = true)
                && this.orgId.equals(formatOrgId(orgId), ignoreCase = true)
    }

    private fun formatOrgId(orgId: String): String {
        return orgId.replace(Regex("[_-]"), ".")
    }
}
