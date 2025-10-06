package no.fintlabs.consumer

import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintLinks
import no.fint.model.resource.FintResource
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.model.*
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.KafkaTestJacksonConfig
import no.fintlabs.consumer.kafka.KafkaUtils
import no.fintlabs.consumer.links.RelationService
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import java.util.concurrent.TimeUnit
import kotlin.test.Ignore
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest
@ActiveProfiles("utdanning-vurdering")
@Import(KafkaTestJacksonConfig::class)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@Ignore // Temporarily ignored due to flakiness, investigating this locally
class RelationServiceTest @Autowired constructor(
    private val cacheService: CacheService,
    private val relationService: RelationService,
    private val kafkaUtils: KafkaUtils,
) {

    // This test does not use embedded kafka, because it tests the persistence of entities after application restart

    private val resourceName = "elevfravar"
    private val relationName = "fravarsregistrering"

    private val resourceId = "superId123"

    private val firstRelationId = "321"
    private val secondRelationId = "54321"

    companion object {
        lateinit var lastResource: FintResource
    }

    @Test
    @Order(1)
    fun `add fravarsregistrering relation to elevfravar`() {
        // Reset state of topic
        kafkaUtils.ensureTopic(resourceName)
        kafkaUtils.purgeTopics(resourceName)

        var resource: FintResource = createElevfravar(resourceId)

        kafkaUtils.produceEntity(resourceId, resourceName, resource)
        awaitUntilResourceInCache()
        resource = getResourceFromCache()

        assertTrue(getRelationLinks(resource).isEmpty())

        processRelation(resourceId, RelationOperation.ADD, firstRelationId)

        assertTrue(getRelationLinks(resource).size == 1)
        lastResource = resource
    }

    @Test
    @Order(2)
    fun `verify resource integrity after added relation`() = verifyResourceIntegrityAfterRestart()

    @Test
    @Order(3)
    fun `add another relation to resource`() {
        awaitUntilResourceInCache()
        awaitUntilLastEntityIsProcessed()

        processRelation(resourceId, RelationOperation.ADD, secondRelationId)
        val resource = getResourceFromCache()

        assertTrue(getRelationLinks(resource).size == 2)
        lastResource = resource
    }

    @Test
    @Order(4)
    fun `verify integrity of resource with two relations`() = verifyResourceIntegrityAfterRestart()

    @Test
    @Order(5)
    fun `delete relation from resource`() {
        awaitUntilResourceInCache()
        awaitUntilLastEntityIsProcessed()

        processRelation(resourceId, RelationOperation.DELETE, firstRelationId)
        val resource = getResourceFromCache()

        assertTrue(getRelationLinks(resource).size == 1)
        lastResource = resource
    }

    @Test
    @Order(6)
    fun `verify that deletion persists after restart`() = verifyResourceIntegrityAfterRestart()

    private fun verifyResourceIntegrityAfterRestart() {
        awaitUntilResourceInCache()
        awaitUntilLastEntityIsProcessed()

        val resourceFromCache = getResourceFromCache()

        assertResourcesAreEqual(resourceFromCache)
        assertRelationsAreEqual(resourceFromCache)
    }

    private fun assertResourcesAreEqual(resource: FintResource) =
        assertEquals(resource, lastResource)

    private fun assertRelationsAreEqual(cachedResource: FintLinks) =
        assertTrue(getRelationLinks(cachedResource).toSet() == getRelationLinks(lastResource).toSet())

    private fun awaitUntilLastEntityIsProcessed() = Thread.sleep(500L)

    private fun getResourceFromCache() =
        cacheService.getCache(resourceName).get(resourceId)

    private fun getRelationLinks(resource: FintLinks) =
        resource.links[relationName] ?: emptyList()

    private fun awaitUntilResourceInCache() =
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            assertNotNull(getResourceFromCache())
        }

    private fun createElevfravar(id: String) =
        ElevfravarResource().apply {
            systemId = Identifikator().apply {
                identifikatorverdi = id
            }
        }

    private fun processRelation(elevfravarId: String, operation: RelationOperation, vararg relationIds: String) =
        relationService.processIfApplicable(
            createRelationUpdate(elevfravarId, operation, *relationIds)
        )

    private fun createRelationUpdate(elevfravarId: String, operation: RelationOperation, vararg relationIds: String) =
        RelationUpdate(
            orgId = "fintlabs-no",
            domainName = "utdanning",
            packageName = "vurdering",
            resource = ResourceRef(
                name = "elevfravar",
                id = ResourceId("", elevfravarId)
            ),
            relation = RelationRef(
                name = relationName,
                ids = relationIds.map {
                    ResourceId(
                        "systemid",
                        it
                    )
                }
            ),
            operation = operation,
            entityRetentionTime = null,
        )

}