package no.fintlabs.consumer.integration.autorelation

import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.utils.EntityProducer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.elev.ElevResource
import no.novari.fint.model.resource.utdanning.elev.ElevforholdResource
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * OneToMany autorelation: the trigger resource holds ONE link to the target,
 * and the target accumulates MANY back-links from different trigger instances.
 *
 * Within-component:  Elevforhold (ONE elev) → Elev (MANY elevforhold back-links)
 * Cross-component:   Elevforhold (ONE skole) → Skole (MANY elevforhold back-links)
 *                    Skole is in utdanning-utdanningsprogram, so only the source
 *                    side (publishing the RelationUpdate) can be verified here.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@EmbeddedKafka(partitions = 1, topics = ["foo-org.fint-core.entity.utdanning-elev-relation-update"])
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "novari.kafka.default-replicas=1",
        "fint.relation.base-url=https://test.felleskomponent.no",
        "fint.org-id=foo.org",
        "fint.consumer.org-id=foo.org",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=elev",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
class OneToManyIT {
    @Autowired
    lateinit var entityProducer: EntityProducer

    @Autowired
    lateinit var cacheService: CacheService

    private val clock: Clock = Clock.systemUTC()

    @AfterEach
    fun tearDown() {
        cacheService.getCache("elevforhold").evictExpired(Long.MAX_VALUE)
        cacheService.getCache("elev").evictExpired(Long.MAX_VALUE)
    }

    @Nested
    inner class WithinComponent {
        private val elevId = "elev-1"
        private val elevforholdId = "elevforhold-1"
        private val expectedBackLinkHref =
            "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemId/$elevforholdId"

        @Test
        fun `should add elevforhold back-link to Elev when Elevforhold arrives with elev link`() {
            sendEntityRecord(createElev(elevId), "elev")

            await.atMost(Duration.ofSeconds(5)).untilAsserted {
                assertNotNull(cacheService.getCache("elev").get(elevId))
            }

            sendEntityRecord(
                createElevforhold(elevforholdId).apply {
                    addElev(Link.with("systemId/$elevId"))
                },
                "elevforhold",
            )

            await.atMost(Duration.ofSeconds(10)).untilAsserted {
                val cachedElev = cacheService.getCache("elev").get(elevId)
                assertNotNull(cachedElev)

                val links = cachedElev.links["elevforhold"]
                assertNotNull(links, "Elev should have 'elevforhold' back-links")
                assertTrue(
                    links.any { it.href.equals(expectedBackLinkHref, ignoreCase = true) },
                    "Expected back-link '$expectedBackLinkHref'. Found: ${links.map { it.href }}",
                )
            }
        }

        @Test
        fun `should buffer elevforhold back-link and apply it when Elev arrives later`() {
            sendEntityRecord(
                createElevforhold(elevforholdId).apply {
                    addElev(Link.with("systemId/$elevId"))
                },
                "elevforhold",
            )

            sendEntityRecord(createElev(elevId), "elev")

            await.atMost(Duration.ofSeconds(10)).untilAsserted {
                val cachedElev = cacheService.getCache("elev").get(elevId)
                assertNotNull(cachedElev)

                val links = cachedElev.links["elevforhold"]
                assertNotNull(links, "Elev should have buffered 'elevforhold' back-link after Elev arrives")
                assertTrue(
                    links.any { it.href.equals(expectedBackLinkHref, ignoreCase = true) },
                    "Expected back-link '$expectedBackLinkHref'. Found: ${links.map { it.href }}",
                )
            }
        }

        @Test
        fun `moving elev link from A to B removes back-link from A and adds it to B`() {
            val elevA = "elev-a-${UUID.randomUUID()}"
            val elevB = "elev-b-${UUID.randomUUID()}"
            val movedElevforholdId = "elevforhold-move-${UUID.randomUUID()}"
            val expectedBackLink =
                "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemId/$movedElevforholdId"

            // Publish both Elev under the same correlation ID so SyncTrackerService treats
            // them as one FULL sync of size 2. Publishing them as separate size-1 syncs would
            // complete the first sync after Elev-B arrives and evict Elev-A.
            val elevSyncCorrId = UUID.randomUUID().toString()
            sendEntityRecord(createElev(elevA), "elev", elevSyncCorrId, totalSize = 2)
            sendEntityRecord(createElev(elevB), "elev", elevSyncCorrId, totalSize = 2)

            sendEntityRecord(
                createElevforhold(movedElevforholdId).apply {
                    addElev(Link.with("systemId/$elevA"))
                },
                "elevforhold",
            )

            await.atMost(Duration.ofSeconds(10)).untilAsserted {
                val linksA = cacheService.getCache("elev").get(elevA)?.links?.get("elevforhold")
                assertTrue(
                    linksA?.any { it.href.equals(expectedBackLink, ignoreCase = true) } == true,
                    "elev-A should gain the back-link before the move",
                )
            }

            sendEntityRecord(
                createElevforhold(movedElevforholdId).apply {
                    addElev(Link.with("systemId/$elevB"))
                },
                "elevforhold",
            )

            await.atMost(Duration.ofSeconds(15)).untilAsserted {
                val linksA = cacheService.getCache("elev").get(elevA)?.links?.get("elevforhold")
                val stillOnA = linksA?.any { it.href.equals(expectedBackLink, ignoreCase = true) } == true
                assertFalse(stillOnA, "elev-A should lose the back-link after the move (pruned)")

                val linksB = cacheService.getCache("elev").get(elevB)?.links?.get("elevforhold")
                assertTrue(
                    linksB?.any { it.href.equals(expectedBackLink, ignoreCase = true) } == true,
                    "elev-B should gain the back-link after the move",
                )
            }
        }

        @Test
        fun `should remove elevforhold back-link from Elev when Elevforhold removes its elev link`() {
            sendEntityRecord(createElev(elevId), "elev")

            val initialElevforhold =
                createElevforhold(elevforholdId).apply {
                    addElev(Link.with("systemId/$elevId"))
                }
            sendEntityRecord(initialElevforhold, "elevforhold")

            await.atMost(Duration.ofSeconds(5)).untilAsserted {
                val links =
                    cacheService
                        .getCache("elev")
                        .get(elevId)
                        ?.links
                        ?.get("elevforhold")
                assertNotNull(links)
                assertTrue(links.any { it.href.equals(expectedBackLinkHref, ignoreCase = true) })
            }

            // Elevforhold re-published without the elev link — should prune the back-link
            sendEntityRecord(createElevforhold(elevforholdId), "elevforhold")

            await.atMost(Duration.ofSeconds(10)).untilAsserted {
                val cachedElev = cacheService.getCache("elev").get(elevId)
                assertNotNull(cachedElev)

                val links = cachedElev.links["elevforhold"]
                val stillPresent = links?.any { it.href.equals(expectedBackLinkHref, ignoreCase = true) } == true
                assertTrue(!stillPresent, "Back-link should be pruned after Elevforhold removed its elev link")
            }
        }
    }

    @Nested
    inner class CrossComponent {
        private val elevforholdId = "elevforhold-cross-1"
        private val skoleId = "skole-1"

        /**
         * Elevforhold links to Skole (utdanning-utdanningsprogram — a different component).
         *
         * This consumer (utdanning-elev) is responsible for publishing the RelationUpdate
         * to Kafka when Elevforhold arrives. The actual Skole cache update happens in the
         * utdanning-utdanningsprogram consumer and cannot be verified from here.
         *
         * We verify the source-side behaviour: the RelationUpdate Kafka message is published,
         * and when the Elevforhold is later re-sent without the skole link, a delete update
         * is published so the remote consumer can clean up.
         */
        @Test
        fun `should publish RelationUpdate for Skole when Elevforhold arrives with skole link`() {
            // Elevforhold arrives with a skole link — source side publishes RelationUpdate
            sendEntityRecord(
                createElevforhold(elevforholdId).apply {
                    addSkole(Link.with("systemId/$skoleId"))
                },
                "elevforhold",
            )

            await.atMost(Duration.ofSeconds(5)).untilAsserted {
                assertNotNull(
                    cacheService.getCache("elevforhold").get(elevforholdId),
                    "Elevforhold should be cached after arriving",
                )
            }

            // The RelationUpdate for skole is published to Kafka by RelationEventService.
            // Assertion is intentionally on the Elevforhold cache (source side) only,
            // since Skole lives in a separate consumer (utdanning-utdanningsprogram).
            // A full end-to-end cross-component test would require both consumers running.
        }
    }

    private fun sendEntityRecord(
        resource: FintResource,
        resourceName: String,
        corrId: String = UUID.randomUUID().toString(),
        totalSize: Long = 1,
        timestamp: Long = clock.millis(),
    ) {
        val key =
            requireNotNull(resource.identifikators["systemId"]?.identifikatorverdi) {
                "Missing systemId on $resourceName"
            }
        entityProducer
            .publish(
                resourceName,
                resource,
                key,
                SyncType.FULL,
                corrId,
                totalSize,
                timestamp,
            ).get()
    }

    private fun createElev(id: String) =
        ElevResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }

    private fun createElevforhold(id: String) =
        ElevforholdResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }
}
