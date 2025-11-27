package no.fintlabs.consumer.config

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationRef
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.ResourceRef
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
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

    @Test
    fun `can deserialize RelationUpdate without throwing`() {
        val relationUpdate = createRelationUpdate() // Kotlin class
        val json = objectMapper.writeValueAsString(relationUpdate)

        assertDoesNotThrow("Deserializing RelationUpdate should not throw when KotlinModule is configured") {
            val deserialized = objectMapper.readValue(json, RelationUpdate::class.java)
            assertEquals(relationUpdate.orgId, deserialized.orgId)
        }
    }

    private fun createRelationUpdate() =
        RelationUpdate(
            orgId = "orgId",
            domainName = "domainName",
            packageName = "pkg",
            resource = ResourceRef("asdf", "asdf"),
            relation = RelationRef("asdf", emptyList()),
            operation = RelationOperation.ADD,
        )
}
