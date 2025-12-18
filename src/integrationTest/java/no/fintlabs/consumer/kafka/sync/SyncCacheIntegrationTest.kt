package no.fintlabs.consumer.kafka.sync

import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.KafkaConstants.*
import no.fintlabs.consumer.kafka.entity.EntityConsumerRecord
import no.fintlabs.consumer.resource.ResourceService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.nio.ByteBuffer
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
    ): EntityConsumerRecord {
        val headers = RecordHeaders()
        headers.add(RecordHeader(SYNC_TYPE, byteArrayOf(type.ordinal.toByte())))
        headers.add(RecordHeader(SYNC_CORRELATION_ID, corrId.toByteArray()))
        headers.add(RecordHeader(SYNC_TOTAL_SIZE, ByteBuffer.allocate(Long.SIZE_BYTES)
            .putLong(totalSize)
            .array()))

        val resource = createResource(resourceId)

        return EntityConsumerRecord(
            resourceName = resourceName,
            resource = createResource(resourceId),
            record = ConsumerRecord<String, Any>(
                "test-topic",
                0,
                0,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME,
                NULL_SIZE,
                NULL_SIZE,
                resourceId,
                resource,
                headers,
                Optional.empty()
            )
        )
    }

    private fun createResource(id: String) =
        ElevfravarResource().apply {
            systemId =
                Identifikator().apply {
                    identifikatorverdi = id
                }
        }
}
