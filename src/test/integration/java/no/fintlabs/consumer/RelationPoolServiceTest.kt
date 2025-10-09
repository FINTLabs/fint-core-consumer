package no.fintlabs.consumer

import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintLinks
import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.model.*
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.KafkaUtils
import no.fintlabs.consumer.links.RelationPoolService
import no.fintlabs.consumer.links.relation.RelationService
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.util.concurrent.TimeUnit
import kotlin.test.assertNotNull


@SpringBootTest
@ActiveProfiles("utdanning-vurdering")
@EmbeddedKafka(
    partitions = 1,
    topics = ["fintlabs-no.fint-core.entity.utdanning-vurdering-elevfravar"]
)
class RelationPoolServiceTest @Autowired constructor(
    private val cacheService: CacheService,
    private val kafkaUtils: KafkaUtils,
) {

    @MockitoSpyBean
    lateinit var relationPoolService: RelationPoolService

    @MockitoSpyBean
    lateinit var relationService: RelationService

    private val resourceName = "elevfravar"
    private val relationName = "fravarsregistrering"
    private val resourceId = "123"
    private val relationId = "321"

    @Test
    fun `add relationupdate to queue if resource doesn't exist`() {
        val relationUpdate = createRelationUpdate(resourceId, RelationOperation.ADD, relationId)
        relationService.processRelationUpdate(relationUpdate)

        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            verify(relationPoolService, times(1)).enqueue(relationUpdate)
        }
    }

    @Test
    fun `process relationupdate if resource appears`() {
        val relationUpdate = createRelationUpdate(resourceId, RelationOperation.ADD, relationId)
        relationService.processRelationUpdate(relationUpdate)

        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            verify(relationPoolService, times(1)).enqueue(relationUpdate)
        }

        var resource = createElevfravar(resourceId)
        assertTrue(getRelations(resource).isEmpty())

        kafkaUtils.produceEntity(resourceId, resourceName, resource)
        awaitUntilResourceInCache()

        resource = getResourceFromCache()

        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            assertTrue(getRelations(resource).isNotEmpty())
        }
    }

    private fun awaitUntilResourceInCache() =
        await().atMost(10, TimeUnit.SECONDS).untilAsserted {
            assertNotNull(getResourceFromCache())
        }

    private fun getResourceFromCache() =
        cacheService.getCache(resourceName).get(resourceId)

    private fun getRelations(resource: FintLinks): List<Link> =
        resource.links.getOrElse(relationName) { emptyList() }

    private fun createElevfravar(id: String = resourceId): FintResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply {
                identifikatorverdi = id
            }
        }


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