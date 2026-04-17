package no.fintlabs.consumer.integration.restart

import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.config.KafkaTestcontainersSupport
import no.fintlabs.consumer.kafka.KafkaTestJacksonConfig
import no.fintlabs.utils.EntityProducer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.elev.ElevResource
import no.novari.fint.model.resource.utdanning.elev.ElevforholdResource
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Import
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * §3.2 — full cold restart: both `EntityConsumer` and `RelationUpdateConsumer` are stopped,
 * the cache is wiped, and the containers are restarted. After replay (seek-to-beginning on
 * both topics), the target entity should be re-cached and its auto-relation back-link must
 * have been re-applied from the relation-update topic.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@ActiveProfiles("utdanning-elev")
@Import(KafkaTestJacksonConfig::class)
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=cold-restart-it",
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
class ColdRestartIT : KafkaTestcontainersSupport() {
    companion object {
        init {
            createComponentTopics(domain = "utdanning", packageName = "elev")
        }
    }

    @Autowired
    lateinit var applicationContext: ApplicationContext

    @Autowired
    lateinit var entityProducer: EntityProducer

    @Autowired
    lateinit var cacheService: CacheService

    @Test
    fun `back-links are rebuilt after a cold restart via entity and relation-update topic replay`() {
        val elevId = "cold-elev-${UUID.randomUUID()}"
        val elevforholdId = "cold-elevforhold-${UUID.randomUUID()}"
        val expectedBackLink =
            "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemId/$elevforholdId"

        publish(
            "elev",
            ElevResource().apply {
                systemId = Identifikator().apply { identifikatorverdi = elevId }
            },
            elevId,
        )
        publish(
            "elevforhold",
            ElevforholdResource().apply {
                systemId = Identifikator().apply { identifikatorverdi = elevforholdId }
                addElev(Link.with("systemId/$elevId"))
            },
            elevforholdId,
        )

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            assertBackLinkPresent(elevId, expectedBackLink, "before restart")
        }

        val entityContainer = container("resourceEntityConsumerFactory")
        val relationUpdateContainer = container("relationUpdateConsumerContainer")

        entityContainer.stop()
        relationUpdateContainer.stop()

        cacheService.getCache("elev").evictExpired(Long.MAX_VALUE)
        cacheService.getCache("elevforhold").evictExpired(Long.MAX_VALUE)
        assertEquals(0, cacheService.getCache("elev").size)
        assertEquals(0, cacheService.getCache("elevforhold").size)

        entityContainer.start()
        relationUpdateContainer.start()
        ContainerTestUtils.waitForAssignment(entityContainer, 1)
        ContainerTestUtils.waitForAssignment(relationUpdateContainer, 1)

        await.atMost(Duration.ofSeconds(30)).untilAsserted {
            assertNotNull(cacheService.getCache("elev").get(elevId), "Elev should be re-cached")
            assertBackLinkPresent(elevId, expectedBackLink, "after cold restart")
        }
    }

    private fun publish(
        resourceName: String,
        resource: no.novari.fint.model.resource.FintResource,
        key: String,
    ) = entityProducer
        .publish(
            resourceName,
            resource,
            key,
            SyncType.FULL,
            UUID.randomUUID().toString(),
            1,
        ).get(10, TimeUnit.SECONDS)

    private fun assertBackLinkPresent(
        elevId: String,
        expectedHref: String,
        when_: String,
    ) {
        val elev = cacheService.getCache("elev").get(elevId)
        assertNotNull(elev, "Elev $elevId should be cached $when_")
        val backLinks = elev.links["elevforhold"]
        assertNotNull(backLinks, "Elev.links['elevforhold'] should exist $when_")
        assertTrue(
            backLinks.any { it.href.equals(expectedHref, ignoreCase = true) },
            "Expected back-link $expectedHref $when_. Got: ${backLinks.map { it.href }}",
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun container(beanName: String): ConcurrentMessageListenerContainer<String, Any> =
        applicationContext.getBean(
            beanName,
            ConcurrentMessageListenerContainer::class.java,
        ) as ConcurrentMessageListenerContainer<String, Any>
}
