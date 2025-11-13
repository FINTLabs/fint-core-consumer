package no.fintlabs.consumer.resource

import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.elev.ElevResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.entity.EntitySync
import no.fintlabs.consumer.kafka.entity.KafkaEntity
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals

@SpringBootTest
@ActiveProfiles("utdanning-elev")
@EmbeddedKafka
class ResourceServiceTest {
    @Autowired
    private lateinit var resourceService: ResourceService

    @Autowired
    private lateinit var cacheService: CacheService

    private val resourceName = "elev"

    @Test
    fun `ensure lastDelivered is set upon new resource`() {
        val resourceId = UUID.randomUUID().toString()
        val oneDayAgo = System.currentTimeMillis() - Duration.ofDays(1).toMillis()
        val kafkaEntity = createKafkaEntity(resourceId, lastModified = oneDayAgo)

        resourceService.handleNewEntity(kafkaEntity)

        assertNotNull(getResourceFromCache(resourceId))
        assertEquals(oneDayAgo, getLastDelivered(resourceId))
    }

    @Test
    fun `ensure received retention times updates cache retention and affects cache eviction`() {
        val resourceIdLongRetention = UUID.randomUUID().toString()
        val resourceIdShortRetention = UUID.randomUUID().toString()
        val kafkaEntityWithLongRetention = createKafkaEntity(resourceIdLongRetention, retentionTime = 100L)
        val kafkaEntityWithShortRetention = createKafkaEntity(resourceIdShortRetention, retentionTime = 1L)

        // Insert entity and set retention to 100 ms
        resourceService.handleNewEntity(kafkaEntityWithLongRetention)

        // The entity should not have expired yet and therefore not be evicted
        triggerCacheEviction()
        assertNotNull(getResourceFromCache(resourceIdLongRetention))

        // Insert entity and set retention to 1 ms
        resourceService.handleNewEntity(kafkaEntityWithShortRetention)

        // With retention time 1 ms for the cache, both entities shall be evictable after more than 1 ms
        Thread.sleep(4)
        triggerCacheEviction()
        assertNull(getResourceFromCache(resourceIdLongRetention))
        assertNull(getResourceFromCache(resourceIdShortRetention))
    }

    @Test
    fun `ensure non-expired resource is not evicted upon cache eviction (default retention is 7 days)`() {
        val resourceId = UUID.randomUUID().toString()
        val sevenDaysInMillis = Duration.ofDays(7).toMillis()
        val kafkaEntity = createKafkaEntity(resourceId, retentionTime = sevenDaysInMillis)

        resourceService.handleNewEntity(kafkaEntity)

        assertNotNull(getResourceFromCache(resourceId))

        triggerCacheEviction()
        Thread.sleep(100)

        assertNotNull(getResourceFromCache(resourceId))
    }

    @Test
    fun mapResourceAndLinksSuccess() {
        val elevResource: ElevResource = createElevResource("123")
        elevResource.addElevforhold(Link.with("systemid/321"))

        val fintResource: FintResource = resourceService.mapResourceAndLinks("elev", elevResource as Any)

        Assertions.assertEquals(
            "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/321",
            fintResource
                .getLinks()["elevforhold"]!!
                .first()
                .href,
        )
    }

    private fun triggerCacheEviction() = getCache().evictOldCacheObjects()

    private fun getResourceFromCache(resourceId: String) = getCache().get(resourceId)

    private fun getCache() = cacheService.getCache(resourceName)

    private fun getLastDelivered(resourceId: String) = getCache().getLastDelivered(resourceId)

    private fun createKafkaEntity(
        resourceId: String,
        resource: FintResource? = createElevResource(resourceId),
        lastModified: Long = System.currentTimeMillis(),
        retentionTime: Long? = null,
    ) = KafkaEntity(
        key = resourceId,
        name = resourceName,
        resource = resource,
        lastModified = lastModified,
        sync =
            EntitySync(
                type = SyncType.FULL,
                corrId = UUID.randomUUID().toString(),
                totalSize = 1L,
            ),
        retentionTime = retentionTime,
    )

    private fun createElevResource(id: String?): ElevResource =
        ElevResource().apply {
            systemId =
                Identifikator().apply {
                    identifikatorverdi = id
                }
        }
}
