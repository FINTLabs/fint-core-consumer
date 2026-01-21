package no.fintlabs.consumer.kafka.sync

import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.entity.ConsumerRecordMetadata
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

        setCacheRetentionTime(1)
        resourceService.processEntityConsumerRecord(kafkaEntity)

        assertNotNull(getResourceFromCache(resourceId))

        triggerCompletedFullSync()
        Thread.sleep(100)

        assertNull(getResourceFromCache(resourceId))
    }

    private fun triggerCompletedFullSync() {
        val completedFullSync = createSync(SyncType.FULL, corrId = UUID.randomUUID().toString(), totalSize = 1L)
        val kafkaEntity = createNewEntity(UUID.randomUUID().toString(), sync = completedFullSync)
        resourceService.processEntityConsumerRecord(kafkaEntity)
    }

    private fun getResourceFromCache(resourceId: String) = getCache().get(resourceId)

    fun setCacheRetentionTime(retentionTimeInMs: Long) = getCache().setRetentionPeriodInMs(retentionTimeInMs)

    private fun getCache() = cacheService.getCache(resourceName)

    private fun createNewEntity(
        resourceId: String,
        resourceName: String = this.resourceName,
        sync: ConsumerRecordMetadata = createSync(),
    ) = KafkaEntity(
        key = resourceId,
        resourceName = resourceName,
        resource = createResource(resourceId),
        lastModified = System.currentTimeMillis(),
        retentionTime = null,
        consumerRecordMetadata = sync,
    )

    private fun createResource(id: String) =
        ElevfravarResource().apply {
            systemId =
                Identifikator().apply {
                    identifikatorverdi = id
                }
        }

    private fun createSync(
        type: SyncType = SyncType.FULL,
        corrId: String = UUID.randomUUID().toString(),
        totalSize: Long = 10L,
    ) = ConsumerRecordMetadata(
        type = type,
        corrId = corrId,
        totalSize = totalSize,
    )
}
