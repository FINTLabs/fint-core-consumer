package no.fintlabs.consumer.integration.autorelation

import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.kafka.RelationUpdateProducer
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.config.KafkaTestcontainersSupport
import no.fintlabs.consumer.kafka.KafkaTestJacksonConfig
import no.fintlabs.utils.EntityProducer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Reproduces the suspected production loss path:
 *
 * 1. Source entity publishes a RelationUpdate (ADD) whose `timestamp` is already older
 *    than `UnresolvedRelationCache` TTL.
 * 2. `RelationUpdateConsumer` tries to buffer the ADD because the target is not yet cached.
 * 3. Caffeine's `expireAfterCreate` computes `remaining = ttl - (now - createdAt)` — negative
 *    for our stale timestamp — and the buffer entry is evicted before it can be drained.
 * 4. Target entity arrives. `reconcileLinks.applyPendingLinks` finds an empty buffer.
 * 5. Back-link never materialises.
 *
 * This path is reachable in production when a long-dormant ADD (surviving on the compacted
 * relation-update topic) is replayed after a consumer restart, or whenever a target takes
 * longer than TTL to arrive on its entity topic.
 *
 * TTL is set to 2 seconds via `fint.consumer.autorelation.buffer.ttl=PT2S` so the test can
 * deterministically manufacture a stale timestamp.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@ActiveProfiles("utdanning-vurdering")
@Import(KafkaTestJacksonConfig::class)
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=stale-buffer-loss-it",
        "novari.kafka.default-replicas=1",
        "fint.relation.base-url=https://test.felleskomponent.no",
        "fint.org-id=fintlabs.no",
        "fint.consumer.org-id=fintlabs.no",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=vurdering",
        "fint.consumer.autorelation.enabled=true",
        "fint.consumer.autorelation.buffer.ttl=PT2S",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
class StaleBufferLossIT : KafkaTestcontainersSupport() {
    companion object {
        init {
            createComponentTopics(domain = "utdanning", packageName = "vurdering")
        }
    }

    @Autowired
    lateinit var entityProducer: EntityProducer

    @Autowired
    lateinit var relationUpdateProducer: RelationUpdateProducer

    @Autowired
    lateinit var cacheService: CacheService

    @Autowired
    lateinit var unresolvedRelationCache: UnresolvedRelationCache

    @Test
    fun `stale RelationUpdate buffered with expired TTL loses its link when target arrives`() {
        val targetResource = "elevfravar"
        val targetId = "stale-${UUID.randomUUID()}"
        val relationName = "fravarsregistrering"
        val sourceResource = "fravarsregistrering"
        val sourceId = "source-${UUID.randomUUID()}"
        val linkHref = "https://test.felleskomponent.no/utdanning/vurdering/fravarsregistrering/systemId/$sourceId"

        val staleTimestamp = System.currentTimeMillis() - Duration.ofSeconds(30).toMillis()
        val stalePayload =
            RelationUpdate(
                targetEntity = EntityDescriptor("utdanning", "vurdering", targetResource),
                targetIds = listOf(targetId),
                binding = RelationBinding(relationName, Link.with(linkHref)),
                operation = RelationOperation.ADD,
                timestamp = staleTimestamp,
            )

        relationUpdateProducer
            .publishRelationUpdate(stalePayload, sourceResource, sourceId)
            .get(10, TimeUnit.SECONDS)

        // Give the RelationUpdateConsumer time to process the ADD and the stillborn
        // buffer entry time to be evicted by Caffeine.
        await.atMost(Duration.ofSeconds(10)).pollDelay(Duration.ofSeconds(3)).untilAsserted {
            unresolvedRelationCache.cleanUp()
            assertNull(
                unresolvedRelationCache
                    .takeRelations(targetResource, targetId, relationName)
                    .takeIf { it.isNotEmpty() },
                "Stale buffer entry should have been evicted; otherwise the TTL math is different than expected",
            )
        }

        entityProducer
            .publish(
                targetResource,
                ElevfravarResource().apply {
                    systemId = Identifikator().apply { identifikatorverdi = targetId }
                },
                targetId,
                SyncType.DELTA,
                UUID.randomUUID().toString(),
                1,
            ).get(10, TimeUnit.SECONDS)

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            assertNotNull(cacheService.getCache(targetResource).get(targetId), "target should be cached")
        }

        // Wait a bit longer in case something eventually applies; then assert the back-link is absent.
        Thread.sleep(2_000)
        val target = cacheService.getCache(targetResource).get(targetId)
        val backLinks = target?.links?.get(relationName)
        val hasLink = backLinks?.any { it.href.equals(linkHref, ignoreCase = true) } == true
        assertFalse(
            hasLink,
            "Expected back-link to be LOST due to stale-buffer bug, but it was present: ${backLinks?.map { it.href }}",
        )
    }
}
