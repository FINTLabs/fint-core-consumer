package no.fintlabs.consumer.integration.crossservice

import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheEvictionService
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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * §6.3 — Evicting a source entity on Service A publishes DELETEs for its relations;
 * Service B must receive those DELETEs and remove the corresponding back-link.
 *
 * In production, `SyncTrackerService` triggers `CacheEvictionService.evictExpired` when a
 * `SyncType.FULL` completes and the post-sync cache contains entries older than the sync
 * start. The test invokes `evictExpired` directly with a future timestamp so every cached
 * Elevforhold is evicted deterministically — mimicking the end-of-full-sync eviction path
 * without having to orchestrate a real full sync.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@ActiveProfiles("utdanning-elev")
@Import(KafkaTestJacksonConfig::class)
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=cross-service-eviction-a-it",
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
class CrossServiceEvictionIT : KafkaTestcontainersSupport() {
    companion object {
        init {
            createComponentTopics(domain = "utdanning", packageName = "elev")
            createComponentTopics(domain = "utdanning", packageName = "utdanningsprogram")
        }
    }

    @Autowired
    lateinit var serviceAProducer: EntityProducer

    @Autowired
    lateinit var serviceACache: CacheService

    @Autowired
    lateinit var serviceAEviction: CacheEvictionService

    private lateinit var serviceBContext: ConfigurableApplicationContext

    private val serviceBCache: CacheService
        get() = serviceBContext.getBean(CacheService::class.java)

    @BeforeAll
    fun startServiceB() {
        serviceBContext =
            SpringApplicationBuilder(Application::class.java)
                .bannerMode(Banner.Mode.OFF)
                .run(
                    "--spring.kafka.bootstrap-servers=${KAFKA.bootstrapServers}",
                    "--spring.kafka.consumer.auto-offset-reset=earliest",
                    "--spring.kafka.consumer.group-id=cross-service-eviction-b-${UUID.randomUUID()}",
                    "--novari.kafka.default-replicas=1",
                    "--fint.relation.base-url=https://test.felleskomponent.no",
                    "--fint.org-id=fintlabs.no",
                    "--fint.consumer.org-id=fintlabs.no",
                    "--fint.consumer.domain=utdanning",
                    "--fint.consumer.package=utdanningsprogram",
                    "--fint.consumer.base-url=https://test.felleskomponent.no",
                    "--fint.consumer.pod-url=http://service-b-eviction-pod",
                    "--fint.consumer.autorelation.enabled=true",
                    "--fint.security.enabled=false",
                    "--server.port=0",
                )
    }

    @AfterAll
    fun stopServiceB() {
        if (::serviceBContext.isInitialized && serviceBContext.isActive) {
            serviceBContext.close()
        }
    }

    @Test
    fun `evicting Elevforhold on Service A drops the back-link on Skole on Service B`() {
        val skoleId = "eviction-skole-${UUID.randomUUID()}"
        val elevforholdId = "eviction-elevforhold-${UUID.randomUUID()}"
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
            val skole = serviceBCache.getCache("skole").get(skoleId)
            assertNotNull(skole, "Service B should cache Skole")
            val backLinks = skole.links["elevforhold"]
            assertNotNull(backLinks, "Skole should gain back-link before eviction")
            assertTrue(
                backLinks.any { it.href.equals(expectedBackLink, ignoreCase = true) },
                "back-link $expectedBackLink should be present before eviction",
            )
        }

        // Evicts every Elevforhold entry older than Long.MAX_VALUE — i.e. everything.
        // The eviction task publishes removeRelations for each evicted entity, which
        // produces a DELETE message on the utdanningsprogram-relation-update topic.
        serviceAEviction.evictExpired("elevforhold", Long.MAX_VALUE)

        await.atMost(Duration.ofSeconds(30)).untilAsserted {
            val skole = serviceBCache.getCache("skole").get(skoleId)
            assertNotNull(skole, "Skole should still be cached on Service B (only its back-link is removed)")
            val backLinks = skole.links["elevforhold"]
            val stillThere = backLinks?.any { it.href.equals(expectedBackLink, ignoreCase = true) } == true
            assertFalse(
                stillThere,
                "Back-link should be dropped after Service A evicts Elevforhold. Got: ${backLinks?.map { it.href }}",
            )
        }
    }
}
