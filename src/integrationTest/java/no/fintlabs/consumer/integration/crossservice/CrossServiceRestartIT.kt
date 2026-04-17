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
 * §6.2 — Closing and re-starting Service B (a full process restart, not just a
 * container stop/start) must rebuild the back-link state by replaying the
 * relation-update topic from offset 0.
 *
 * Flow:
 *   1. Service A publishes Skole → Service B caches it.
 *   2. Service A publishes Elevforhold → ADD on utdanningsprogram-relation-update.
 *   3. Service B's RelationUpdateConsumer applies the back-link.
 *   4. Service B's Spring context is closed and replaced. Its in-memory cache and
 *      unresolved-relation buffer are gone; its consumer group gets a new UUID
 *      suffix, so seek-to-beginning re-reads both topics.
 *   5. Expect: the Skole cached by the new Service B has the back-link again.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@ActiveProfiles("utdanning-elev")
@Import(KafkaTestJacksonConfig::class)
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=cross-service-restart-a-it",
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
class CrossServiceRestartIT : KafkaTestcontainersSupport() {
    companion object {
        init {
            createComponentTopics(domain = "utdanning", packageName = "elev")
            createComponentTopics(domain = "utdanning", packageName = "utdanningsprogram")
        }
    }

    @Autowired
    lateinit var serviceAProducer: EntityProducer

    private lateinit var serviceBContext: ConfigurableApplicationContext

    private val serviceBCache: CacheService
        get() = serviceBContext.getBean(CacheService::class.java)

    @BeforeAll
    fun startServiceB() {
        serviceBContext = startServiceBContext()
    }

    @AfterAll
    fun stopServiceB() {
        if (::serviceBContext.isInitialized && serviceBContext.isActive) {
            serviceBContext.close()
        }
    }

    @Test
    fun `Service B full restart rebuilds Skole cache and re-applies back-link via topic replay`() {
        val skoleId = "restart-skole-${UUID.randomUUID()}"
        val elevforholdId = "restart-elevforhold-${UUID.randomUUID()}"
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
            assertBackLinkPresent(skoleId, expectedBackLink, when_ = "before restart")
        }

        serviceBContext.close()
        serviceBContext = startServiceBContext()

        await.atMost(Duration.ofSeconds(30)).untilAsserted {
            assertBackLinkPresent(skoleId, expectedBackLink, when_ = "after Service B cold restart")
        }
    }

    private fun assertBackLinkPresent(
        skoleId: String,
        expectedHref: String,
        when_: String,
    ) {
        val skole = serviceBCache.getCache("skole").get(skoleId)
        assertNotNull(skole, "Skole $skoleId should be cached on Service B $when_")
        val backLinks = skole.links["elevforhold"]
        assertNotNull(backLinks, "Skole should have 'elevforhold' back-link $when_")
        assertTrue(
            backLinks.any { it.href.equals(expectedHref, ignoreCase = true) },
            "Expected back-link $expectedHref $when_. Got: ${backLinks.map { it.href }}",
        )
    }

    private fun startServiceBContext(): ConfigurableApplicationContext =
        SpringApplicationBuilder(Application::class.java)
            .bannerMode(Banner.Mode.OFF)
            .run(
                "--spring.kafka.bootstrap-servers=${KAFKA.bootstrapServers}",
                "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--spring.kafka.consumer.group-id=cross-service-restart-b-${UUID.randomUUID()}",
                "--novari.kafka.default-replicas=1",
                "--fint.relation.base-url=https://test.felleskomponent.no",
                "--fint.org-id=fintlabs.no",
                "--fint.consumer.org-id=fintlabs.no",
                "--fint.consumer.domain=utdanning",
                "--fint.consumer.package=utdanningsprogram",
                "--fint.consumer.base-url=https://test.felleskomponent.no",
                "--fint.consumer.pod-url=http://service-b-restart-pod",
                "--fint.consumer.autorelation.enabled=true",
                "--fint.security.enabled=false",
                "--server.port=0",
            )
}
