package no.fintlabs.consumer.integration.restart

import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.config.KafkaTestcontainersSupport
import no.fintlabs.consumer.kafka.KafkaTestJacksonConfig
import no.fintlabs.utils.EntityProducer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
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

/**
 * §3.1 — proves `EntityConsumer` rebuilds the in-memory cache from the entity topic after a
 * restart. The container is configured with `seekToBeginningOnAssignment`, so every new
 * partition assignment (including the one triggered by start → stop → start in a single JVM)
 * must replay the topic from offset 0.
 *
 * The test isolates entity replay from autorelation by disabling `fint.consumer.autorelation.enabled`,
 * so the `AutoRelationEntityConsumer` and `RelationUpdateConsumer` beans are not registered.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@ActiveProfiles("utdanning-vurdering")
@Import(KafkaTestJacksonConfig::class)
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=entity-restart-it",
        "novari.kafka.default-replicas=1",
        "fint.relation.base-url=https://test.felleskomponent.no",
        "fint.org-id=fintlabs.no",
        "fint.consumer.org-id=fintlabs.no",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=vurdering",
        "fint.consumer.autorelation.enabled=false",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
class EntityConsumerRestartIT : KafkaTestcontainersSupport() {
    companion object {
        init {
            createComponentTopics(domain = "utdanning", packageName = "vurdering")
        }
    }

    @Autowired
    lateinit var applicationContext: ApplicationContext

    @Autowired
    lateinit var entityProducer: EntityProducer

    @Autowired
    lateinit var cacheService: CacheService

    @Test
    fun `cache is rebuilt after EntityConsumer restart via seek-to-beginning`() {
        val resourceName = "elevfravar"
        val ids = (1..3).map { "restart-${UUID.randomUUID()}" }

        ids.forEach { id ->
            entityProducer
                .publish(
                    resourceName,
                    ElevfravarResource().apply {
                        systemId = Identifikator().apply { identifikatorverdi = id }
                    },
                    id,
                    SyncType.FULL,
                    UUID.randomUUID().toString(),
                    ids.size.toLong(),
                ).get(10, TimeUnit.SECONDS)
        }

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            ids.forEach { id ->
                assertNotNull(cacheService.getCache(resourceName).get(id), "$id should be cached before restart")
            }
        }

        val entityContainer = entityConsumerContainer()
        entityContainer.stop()

        val cache = cacheService.getCache(resourceName)
        cache.evictExpired(Long.MAX_VALUE)
        assertEquals(0, cache.size, "cache should be empty after explicit eviction, simulating a cold restart")

        entityContainer.start()
        ContainerTestUtils.waitForAssignment(entityContainer, 1)

        await.atMost(Duration.ofSeconds(30)).untilAsserted {
            ids.forEach { id ->
                assertNotNull(
                    cacheService.getCache(resourceName).get(id),
                    "$id should be re-cached after seek-to-beginning replay",
                )
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun entityConsumerContainer(): ConcurrentMessageListenerContainer<String, Any> =
        applicationContext.getBean(
            "resourceEntityConsumerFactory",
            ConcurrentMessageListenerContainer::class.java,
        ) as ConcurrentMessageListenerContainer<String, Any>
}
