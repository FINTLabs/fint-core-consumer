package no.fintlabs.consumer.integration.crossservice

import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.config.KafkaTestcontainersSupport
import no.fintlabs.consumer.kafka.KafkaTestJacksonConfig
import no.fintlabs.utils.EntityProducer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.elev.ElevforholdResource
import no.novari.fint.model.resource.utdanning.utdanningsprogram.SkoleResource
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.Banner
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Import
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue

/**
 * §6.1 — Cross-service back-link flow with two independent Spring contexts sharing one
 * Testcontainers Kafka broker.
 *
 * Service A (utdanning-elev) is the @SpringBootTest context. It publishes Elevforhold with
 * a `skole` link; its `AutoRelationEntityConsumer` then publishes an ADD to the
 * `utdanning-utdanningsprogram-relation-update` topic because Skole lives in a different
 * component.
 *
 * Service B (utdanning-utdanningsprogram) is started programmatically via
 * `SpringApplicationBuilder.run(...)` with CLI args — CLI args take higher precedence than
 * `application.yaml`, so this reliably reconfigures the second context's domain/package.
 * Service B's `RelationUpdateConsumer` listens on the utdanningsprogram relation-update topic
 * and applies the back-link to its own Skole cache.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@ActiveProfiles("utdanning-elev")
@Import(KafkaTestJacksonConfig::class)
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=cross-service-a-it",
        "novari.kafka.default-replicas=1",
        "fint.relation.base-url=https://test.felleskomponent.no",
        "fint.org-id=fintlabs.no",
        "fint.consumer.org-id=fintlabs.no",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=elev",
        "fint.consumer.autorelation.enabled=true",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CrossServiceBackLinkIT : KafkaTestcontainersSupport() {
    companion object {
        init {
            createComponentTopics(domain = "utdanning", packageName = "elev")
            createComponentTopics(domain = "utdanning", packageName = "utdanningsprogram")
        }
    }

    @Autowired
    lateinit var serviceAProducer: EntityProducer

    private lateinit var serviceBContext: ConfigurableApplicationContext
    private lateinit var serviceBCache: CacheService

    @BeforeAll
    fun startServiceB() {
        serviceBContext =
            SpringApplicationBuilder(Application::class.java)
                .bannerMode(Banner.Mode.OFF)
                .run(
                    "--spring.kafka.bootstrap-servers=${KAFKA.bootstrapServers}",
                    "--spring.kafka.consumer.auto-offset-reset=earliest",
                    "--spring.kafka.consumer.group-id=cross-service-b-${UUID.randomUUID()}",
                    "--novari.kafka.default-replicas=1",
                    "--fint.relation.base-url=https://test.felleskomponent.no",
                    "--fint.org-id=fintlabs.no",
                    "--fint.consumer.org-id=fintlabs.no",
                    "--fint.consumer.domain=utdanning",
                    "--fint.consumer.package=utdanningsprogram",
                    "--fint.consumer.base-url=https://test.felleskomponent.no",
                    "--fint.consumer.pod-url=http://service-b-test-pod",
                    "--fint.consumer.autorelation.enabled=true",
                    "--fint.security.enabled=false",
                    "--server.port=0",
                )
        serviceBCache = serviceBContext.getBean(CacheService::class.java)
    }

    @AfterAll
    fun stopServiceB() {
        if (::serviceBContext.isInitialized) {
            serviceBContext.close()
        }
    }

    @Test
    fun `Service A publishes Elevforhold, Service B receives back-link on Skole`() {
        val skoleId = "cross-skole-${UUID.randomUUID()}"
        val elevforholdId = "cross-elevforhold-${UUID.randomUUID()}"
        val expectedBackLink =
            "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemId/$elevforholdId"

        serviceAProducer
            .publish(
                "skole",
                SkoleResource().apply {
                    systemId = Identifikator().apply { identifikatorverdi = skoleId }
                },
                skoleId,
                SyncType.DELTA,
                UUID.randomUUID().toString(),
                1,
                domainName = "utdanning",
                packageName = "utdanningsprogram",
            ).get(10, TimeUnit.SECONDS)

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            assertNotNull(
                serviceBCache.getCache("skole").get(skoleId),
                "Service B should cache Skole $skoleId from the shared entity topic",
            )
        }

        serviceAProducer
            .publish(
                "elevforhold",
                ElevforholdResource().apply {
                    systemId = Identifikator().apply { identifikatorverdi = elevforholdId }
                    addSkole(Link.with("systemId/$skoleId"))
                },
                elevforholdId,
                SyncType.DELTA,
                UUID.randomUUID().toString(),
                1,
            ).get(10, TimeUnit.SECONDS)

        await.atMost(Duration.ofSeconds(30)).untilAsserted {
            val skole = serviceBCache.getCache("skole").get(skoleId)
            assertNotNull(skole, "Skole should still be cached on Service B")
            val backLinks = skole.links["elevforhold"]
            assertNotNull(backLinks, "Skole should gain an 'elevforhold' back-link from the cross-service relation update")
            assertTrue(
                backLinks.any { it.href.equals(expectedBackLink, ignoreCase = true) },
                "Expected back-link $expectedBackLink. Got: ${backLinks.map { it.href }}",
            )
        }
    }
}
