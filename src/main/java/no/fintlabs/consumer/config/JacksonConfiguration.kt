package no.fintlabs.consumer.config

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.ISO8601DateFormat
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import jakarta.annotation.PostConstruct
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder

@Configuration
open class JacksonConfiguration(
    private val objectMapper: ObjectMapper,
) {
    companion object {
        /**
         * ISO-8601 Zulu format required by Felles Personmodell → fødselsdato:
         * https://informasjonsmodell.felleskomponent.no/docs/felles_person/~fodselsdato
         * https://en.wikipedia.org/wiki/ISO_8601
         *
         * Must match EXACTLY:
         *   YYYY-MM-DDThh:mm:ssZ
         *
         * - Always UTC
         * - Always ends with 'Z'
         * - No milliseconds
         */
        const val ISO_8601_ZULU_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    }

    @PostConstruct
    open fun customizeObjectMapper() { // Old configuration
        objectMapper.setDateFormat(ISO8601DateFormat()).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
        objectMapper.registerModule(KotlinModule.Builder().build())
    }

    open fun jackson2ObjectMapperBuilder(): Jackson2ObjectMapperBuilder =
        Jackson2ObjectMapperBuilder()
            .failOnUnknownProperties(false)
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .modules(JavaTimeModule(), KotlinModule.Builder().build())
            .simpleDateFormat(ISO_8601_ZULU_PATTERN)
            .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    open fun objectMapper(builder: Jackson2ObjectMapperBuilder): ObjectMapper = builder.build()
}
