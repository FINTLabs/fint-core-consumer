package no.fintlabs.consumer.integration

import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.kafka.RelationUpdateProducer
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationState
import no.fintlabs.cache.CacheService
import no.fintlabs.utils.EntityProducer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@EmbeddedKafka(
    partitions = 1,
    topics = [
        "foo-org.fint-core.entity.utdanning-vurdering",
        "foo-org.fint-core.entity.utdanning-vurdering-relation-update",
    ],
)
@ActiveProfiles("utdanning-vurdering")
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=autorelation-service-it",

        "novari.kafka.default-replicas=1",
        "fint.relation.base-url=https://test.felleskomponent.no",
        "fint.org-id=foo.org",
        "fint.consumer.org-id=foo.org",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=vurdering",
        "fint.consumer.autorelation.enabled=true",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
class AutoRelationIT {
    @Autowired
    lateinit var entityProducer: EntityProducer

    @Autowired
    lateinit var cacheService: CacheService

    @Autowired
    lateinit var relationUpdateProducer: RelationUpdateProducer

    @Autowired
    lateinit var unresolvedRelationCache: UnresolvedRelationCache

    private val resourceName = "elevfravar"
    private val relationName = "fravarsregistrering"

    @AfterEach
    fun tearDown() {
        cacheService.getCache(resourceName).evictExpired(Long.MAX_VALUE)
        unresolvedRelationCache.cleanUp()
    }

    @Test
    fun `applies relation state to a cached resource`() {
        val resourceId = UUID.randomUUID().toString()

        sendEntityRecord(resourceId, createResource(resourceId))
        publishRelationState(listOf(resourceId), Link.with("systemid/child-1"))

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertLinkWithSuffixExists(links, "systemid/child-1")
        }
    }

    @Test
    fun `preserves cached relations when a fresh resource arrives`() {
        val resourceId = UUID.randomUUID().toString()
        val inheritedLink = Link.with("systemid/child-existing")

        val oldResource =
            createResource(resourceId).apply {
                links[relationName] = mutableListOf(inheritedLink)
            }
        sendEntityRecord(resourceId, oldResource)

        val newResource = createResource(resourceId)
        assertNull(newResource.links[relationName])

        sendEntityRecord(resourceId, newResource)

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertLinkWithSuffixExists(links, "systemid/child-existing")
        }
    }

    @Test
    fun `buffers relation state until the target resource arrives`() {
        val resourceId = UUID.randomUUID().toString()

        publishRelationState(listOf(resourceId), Link.with("systemid/child-pending"))

        sendEntityRecord(resourceId, createResource(resourceId))

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertLinkWithSuffixExists(links, "systemid/child-pending")
        }
    }

    @Test
    fun `removes an existing relation when an empty state is received`() {
        val resourceId = UUID.randomUUID().toString()
        val linkToDelete = Link.with("systemid/child-to-delete")

        val resource = createResource(resourceId).apply { addLink(relationName, linkToDelete) }
        sendEntityRecord(resourceId, resource)

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            assertNotNull(cacheService.getCache(resourceName).get(resourceId))
        }

        publishRelationState(emptyList(), linkToDelete)

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNull(links, "The relation key should be removed when the last link is deleted")
        }
    }

    @Test
    fun `cancels a pending link when an empty state arrives before the target`() {
        val resourceId = UUID.randomUUID().toString()
        val link = Link.with("systemid/cancel-me")
        val sourceRef = "systemid/cancel-me"

        publishRelationState(listOf(resourceId), link)
        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            assertTrue(
                unresolvedRelationCache.findPendingTargets(resourceName, relationName, sourceRef).contains(resourceId),
            )
        }

        publishRelationState(emptyList(), link)
        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            assertTrue(unresolvedRelationCache.findPendingTargets(resourceName, relationName, sourceRef).isEmpty())
        }

        sendEntityRecord(resourceId, createResource(resourceId))

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)
            assertTrue(
                cachedResource.links[relationName].isNullOrEmpty(),
                "The cancelled link must not be applied once the target arrives",
            )
        }
    }

    @Test
    fun `applies multiple pending sources when the resource appears`() {
        val resourceId = UUID.randomUUID().toString()

        publishRelationState(listOf(resourceId), Link.with("systemid/1"))
        publishRelationState(listOf(resourceId), Link.with("systemid/2"))

        sendEntityRecord(resourceId, createResource(resourceId))

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            assertNotNull(links)
            assertEquals(2, links.size)
            assertLinkWithSuffixExists(links, "systemid/1")
            assertLinkWithSuffixExists(links, "systemid/2")
        }
    }

    @Test
    fun `does not duplicate buffered links when the same state arrives twice`() {
        val resourceId = UUID.randomUUID().toString()
        val link = Link.with("systemid/duplicate")

        publishRelationState(listOf(resourceId), link)
        publishRelationState(listOf(resourceId), link)

        sendEntityRecord(resourceId, createResource(resourceId))

        await.atMost(Duration.ofSeconds(15)).untilAsserted {
            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            assertNotNull(links)
            assertEquals(1, links.size)
        }
    }

    private fun createResource(id: String): ElevfravarResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }

    private fun sendEntityRecord(
        resourceId: String,
        resource: FintResource,
    ) {
        entityProducer
            .publish(resourceName, resource, resourceId, SyncType.FULL, UUID.randomUUID().toString(), 1)
            .get(10, TimeUnit.SECONDS)
    }

    /**
     * Publishes a relation-state snapshot for the source identified by [link], naming the full set
     * of [targetIds] (this consumer's [resourceName]) that should hold a back-link to it. An empty
     * [targetIds] removes the source's back-link from every target that currently has it.
     */
    private fun publishRelationState(
        targetIds: List<String>,
        link: Link,
    ) {
        val state =
            RelationState(
                targetEntity = EntityDescriptor("utdanning", "vurdering", resourceName),
                targetIds = targetIds,
                binding = RelationBinding(relationName, link),
            )
        val sourceId = link.href!!.substringAfterLast("/")
        relationUpdateProducer.publish(state, relationName, sourceId).get(10, TimeUnit.SECONDS)
    }

    private fun assertLinkWithSuffixExists(
        links: List<Link>,
        suffix: String,
    ) {
        val match = links.any { it.href?.endsWith(suffix, ignoreCase = true) == true }
        assertTrue(match, "Expected link ending with '$suffix' was not found. Found: ${links.map { it.href }}")
    }
}
