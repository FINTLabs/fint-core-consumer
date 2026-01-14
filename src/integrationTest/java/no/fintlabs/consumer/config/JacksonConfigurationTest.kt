package no.fintlabs.consumer.config

import com.fasterxml.jackson.annotation.JsonFilter
import com.fasterxml.jackson.databind.ObjectMapper
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.elev.ElevResource
import no.fintlabs.autorelation.model.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.util.*
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("utdanning-elev")
@EmbeddedKafka
class JacksonConfigurationTest {
    @Autowired
    lateinit var objectMapper: ObjectMapper

    @JsonFilter("opaFilter")
    data class MockResourceWithFilter(
        val navn: String,
        val systemId: Identifikator,
    )

    @Test
    fun `should verify serialization succeeds even when filter is missing`() {
        val resource =
            ElevResource().apply {
                systemId =
                    Identifikator().apply {
                        identifikatorverdi = UUID.randomUUID().toString()
                    }
            }
        val request = createRelationRequest(resource)

        assertDoesNotThrow {
            val json = objectMapper.writeValueAsString(request)
            assertTrue(json.contains(request.orgId))
        }
    }

    @Test
    fun `date is using ISO-8601 format`() {
        val date = Date(1672531200000L) // 2023-01-01T00:00:00.000 UTC
        val json = objectMapper.writeValueAsString(date)

        // Matches "YYYY-MM-DDThh:mm:ss.sssZ"
        val iso8601Regex = Regex("^\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z\"$")
        assertTrue(iso8601Regex.matches(json), "Expected ISO-8601 format, but got: $json")
    }

    @Test
    fun `can deserialize Kotlin class without throwing`() {
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
            targetEntity = EntityDescriptor("domain", "pkg", "resource"),
            targetId = "123",
            binding = RelationBinding("asdf", Link()),
            operation = RelationOperation.ADD,
        )

    private fun createRelationRequest(resource: FintResource) =
        RelationEvent(
            sourceEntity =
                EntityDescriptor(
                    domainName = "utdanning",
                    packageName = "elev",
                    resourceName = "elev",
                ),
            orgId = "fintlabs.no",
            operation = RelationOperation.ADD,
            sourceId = "123",
            sourceData = resource,
        )
}
