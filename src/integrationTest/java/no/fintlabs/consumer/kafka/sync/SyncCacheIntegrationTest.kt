package no.fintlabs.consumer.kafka.sync

import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.entity.KafkaEntity
import no.fintlabs.consumer.resource.ResourceService
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.util.*

@SpringBootTest
@ActiveProfiles("utdanning-vurdering")
@EmbeddedKafka(partitions = 1, topics = ["fintlabs-no.fint-core.event.sync-status"])
class SyncCacheIntegrationTest {
    @Autowired
    private lateinit var resourceService: ResourceService

    @Autowired
    private lateinit var cacheService: CacheService

    private val resourceName = "elevfravar"

    @Test
    fun `expired resource is removed upon completed full-sync`() {
        val resourceId = UUID.randomUUID().toString()
        val kafkaEntity = createNewEntity(resourceId)

        resourceService.processEntityConsumerRecord(kafkaEntity)

        assertNotNull(getResourceFromCache(resourceId))

        triggerCompletedFullSync()
        Thread.sleep(100)

        assertNull(getResourceFromCache(resourceId))
    }

    private fun triggerCompletedFullSync() {
        val kafkaEntity = createNewEntity(UUID.randomUUID().toString(), type = SyncType.FULL, corrId = UUID.randomUUID().toString(), totalSize = 1L)
        resourceService.processEntityConsumerRecord(kafkaEntity)
    }

    private fun getResourceFromCache(resourceId: String) = getCache().get(resourceId)

    private fun getCache() = cacheService.getCache(resourceName)

    private fun createNewEntity(
        resourceId: String,
        resourceName: String = this.resourceName,
        type: SyncType = SyncType.FULL,
        corrId: String = UUID.randomUUID().toString(),
        totalSize: Long = 10L,
    ) = KafkaEntity(
        key = resourceId,
        resourceName = resourceName,
        resource = createResource(resourceId),
        timestamp = System.currentTimeMillis(),
        type = type,
        corrId = corrId,
        totalSize = totalSize,
    )

    private fun createResource(id: String) =
        ElevfravarResource().apply {
            systemId =
                Identifikator().apply {
                    identifikatorverdi = id
                }
        }
}
