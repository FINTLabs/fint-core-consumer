package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

class KafkaEntityTest {
    private val consumerRecord: ConsumerRecord<String, Any> = mockk()

    private val resourceKey = UUID.randomUUID().toString()
    private val resourceName = "elevfravar"
    private val lastModified = System.currentTimeMillis()
    private val syncTotalSize = 100L
    private val syncCorrId = UUID.randomUUID().toString()
    private val resource = ElevfravarResource()

    private val currentTimeByteArray =
        ByteBuffer
            .allocate(8)
            .order(ByteOrder.BIG_ENDIAN)
            .putLong(lastModified)
            .array()

    private val syncCorrIdByteArray = syncCorrId.toByteArray()
    private val totalSizeByteArray =
        ByteBuffer
            .allocate(8)
            .order(ByteOrder.BIG_ENDIAN)
            .putLong(syncTotalSize)
            .array()

    @Test
    fun `fields are mapped correctly`() {
        val syncType = SyncType.FULL

        stubConsumerRecord(syncType = syncType)

        val entity = createKafkaEntity()

        assertEquals(resourceName, entity.resourceName)
        assertEquals(resourceKey, entity.key)
        assertEquals(resource, entity.resource)
        assertEquals(lastModified, entity.timestamp)
        assertEquals(syncCorrId, entity.corrId)
        assertEquals(syncTotalSize, entity.totalSize)
        assertEquals(syncType, entity.type)
    }

    @Test
    fun `fullSync type is set and converted`() {
        stubConsumerRecord(syncType = SyncType.FULL)

        val entity = createKafkaEntity()

        assertEquals(SyncType.FULL, entity.type)
    }

    @Test
    fun `deltaSync type is set and converted`() {
        stubConsumerRecord(syncType = SyncType.DELTA)

        val entity = createKafkaEntity()

        assertEquals(SyncType.DELTA, entity.type)
    }

    @Test
    fun `deleteSync type is set and converted`() {
        stubConsumerRecord(syncType = SyncType.DELETE)

        val entity = createKafkaEntity()

        assertEquals(SyncType.DELETE, entity.type)
    }

    @Test
    fun `unknown syncType ordinal throws IllegalArgumentException`() {
        stubConsumerRecord(syncTypeOrdinal = 127)

        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    @Test
    fun `negative syncType index throws IllegalArgumentException`() {
        stubConsumerRecord(syncTypeOrdinal = -5)
        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    private fun stubConsumerRecord(
        excludedHeader: String? = null,
        syncType: SyncType = SyncType.FULL,
    ) = stubConsumerRecord(excludedHeader, syncType.ordinal)

    private fun stubConsumerRecord(
        excludedHeader: String? = null,
        syncTypeOrdinal: Int = 0,
    ) {
        every { consumerRecord.key() } returns resourceKey
        every { consumerRecord.timestamp() } returns lastModified
        every { consumerRecord.headers() } returns RecordHeaders().apply {
            add(SYNC_TYPE, byteArrayOf(syncTypeOrdinal.toByte()))
            add(LAST_MODIFIED, currentTimeByteArray)
            add(SYNC_CORRELATION_ID, syncCorrIdByteArray)
            add(SYNC_TOTAL_SIZE, totalSizeByteArray)
        }.also { header -> excludedHeader?.let { header.remove(it) } }
    }

    private fun createKafkaEntity() =
        KafkaEntity.create(
            resourceName = resourceName,
            resource = resource,
            record = consumerRecord,
        )
}
