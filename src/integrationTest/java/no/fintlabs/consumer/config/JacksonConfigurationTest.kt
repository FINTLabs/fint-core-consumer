package no.fintlabs.consumer.config

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import org.springframework.test.context.TestPropertySource
import java.util.*
import kotlin.test.assertTrue

@SpringBootTest
@TestPropertySource(
    properties = [
        "fint.relation.base-url=https://testorg.no",
        "fint.consumer.domain=domain",
        "fint.org-id=foo.org",
        "fint.consumer.package=package",
    ],
)
class JacksonConfigurationTest {
    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Test
    fun `date is using ISO-8601 format`() {
        val date = Date(1672531200000L) // 2023-01-01T00:00:00.000 UTC
        val json = objectMapper.writeValueAsString(date)

        // Matches "YYYY-MM-DDThh:mm:ss.sss+00:00" or "YYYY-MM-DDThh:mm:ss.sssZ"
        val iso8601Regex = Regex("^\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}([+-]\\d{2}:\\d{2}|Z)\"$")
        assertTrue(iso8601Regex.matches(json), "Expected ISO-8601 format, but got: $json")
    }
}
