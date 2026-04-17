package no.fintlabs.consumer.integration

import no.fintlabs.Application
import no.fintlabs.config.KafkaTestcontainersSupport
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

/**
 * Smoke test verifying the Spring context starts against a Testcontainers Kafka broker
 * with the required topics pre-created. Mirrors production behaviour (no auto-topic creation).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@ActiveProfiles("utdanning-vurdering")
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=testcontainers-smoke-it",
        "novari.kafka.default-replicas=1",
        "fint.relation.base-url=https://test.felleskomponent.no",
        "fint.org-id=fintlabs.no",
        "fint.consumer.org-id=fintlabs.no",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=vurdering",
        "fint.consumer.autorelation.enabled=true",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
class TestcontainersKafkaSmokeIT : KafkaTestcontainersSupport() {
    companion object {
        init {
            createComponentTopics(domain = "utdanning", packageName = "vurdering")
        }
    }

    @Test
    fun `context starts with pre-created topics`() {
        // Reaching this point means the Spring context booted successfully against
        // the Testcontainers Kafka broker with the four required topics in place.
    }
}
