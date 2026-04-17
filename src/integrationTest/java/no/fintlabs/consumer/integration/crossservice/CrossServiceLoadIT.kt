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
import kotlin.test.assertEquals

/**
 * §6.4 — Interleaved load across services. Many Skole/Elevforhold pairs are published in
 * alternating order on Service A; Service B must receive every back-link without loss.
 *
 * This is the stress variant of §6.1 — the single-pair happy path is already covered by
 * `CrossServiceBackLinkIT`. The aim here is to catch concurrency-, ordering-, or
 * buffering-related losses that only surface under volume.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@ActiveProfiles("utdanning-elev")
@Import(KafkaTestJacksonConfig::class)
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=cross-service-load-a-it",
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
class CrossServiceLoadIT : KafkaTestcontainersSupport() {
    companion object {
        init {
            createComponentTopics(domain = "utdanning", packageName = "elev")
            createComponentTopics(domain = "utdanning", packageName = "utdanningsprogram")
        }

        private const val PAIR_COUNT = 50
    }

    @Autowired
    lateinit var serviceAProducer: EntityProducer

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
                    "--spring.kafka.consumer.group-id=cross-service-load-b-${UUID.randomUUID()}",
                    "--novari.kafka.default-replicas=1",
                    "--fint.relation.base-url=https://test.felleskomponent.no",
                    "--fint.org-id=fintlabs.no",
                    "--fint.consumer.org-id=fintlabs.no",
                    "--fint.consumer.domain=utdanning",
                    "--fint.consumer.package=utdanningsprogram",
                    "--fint.consumer.base-url=https://test.felleskomponent.no",
                    "--fint.consumer.pod-url=http://service-b-load-pod",
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
    fun `every Skole receives its back-link when N pairs are published interleaved`() {
        val runId = UUID.randomUUID().toString().take(8)
        val skoleIds = (1..PAIR_COUNT).map { "load-$runId-skole-$it" }
        val elevforholdIds = (1..PAIR_COUNT).map { "load-$runId-elevforhold-$it" }

        val skoleSyncCorrId = UUID.randomUUID().toString()
        val elevforholdSyncCorrId = UUID.randomUUID().toString()

        for (i in 0 until PAIR_COUNT) {
            val skoleId = skoleIds[i]
            val elevforholdId = elevforholdIds[i]

            serviceAProducer
                .publish(
                    "skole",
                    SkoleResource().apply {
                        systemId = Identifikator().apply { identifikatorverdi = skoleId }
                    },
                    skoleId,
                    SyncType.FULL,
                    skoleSyncCorrId,
                    PAIR_COUNT.toLong(),
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
                    SyncType.FULL,
                    elevforholdSyncCorrId,
                    PAIR_COUNT.toLong(),
                ).get(10, TimeUnit.SECONDS)
        }

        await.atMost(Duration.ofSeconds(60)).untilAsserted {
            val missing = mutableListOf<String>()
            for (i in 0 until PAIR_COUNT) {
                val skoleId = skoleIds[i]
                val expectedBackLink =
                    "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemId/${elevforholdIds[i]}"
                val skole = serviceBCache.getCache("skole").get(skoleId)
                assertNotNull(skole, "Service B should have cached Skole $skoleId")
                val backLinks = skole.links["elevforhold"]
                val hasLink = backLinks?.any { it.href.equals(expectedBackLink, ignoreCase = true) } == true
                if (!hasLink) {
                    missing.add(
                        "Skole $skoleId missing expected link $expectedBackLink (has: ${backLinks?.map { it.href } ?: "no backLinks"})",
                    )
                }
            }
            assertEquals(
                emptyList(),
                missing,
                "${missing.size}/$PAIR_COUNT Skole entries are missing their back-link. First few: ${missing.take(5)}",
            )
        }
    }
}
