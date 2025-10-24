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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

class ResourceKafkaEntityTest {

    private val consumerRecord: ConsumerRecord<String, Any> = mockk()

    private val resourceKey = UUID.randomUUID().toString()
    private val resourceName = "elevfravar"
    private val lastModified = System.currentTimeMillis()
    private val syncTotalSize = 100L
    private val syncCorrId = UUID.randomUUID().toString()
    private val resource = ElevfravarResource()

    private val currentTimeByteArray =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(lastModified).array()
    private val syncCorrIdByteArray = syncCorrId.toByteArray()
    private val totalSizeByteArray = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(syncTotalSize).array()

    @BeforeEach
    fun setUp() {
        every { consumerRecord.key() } returns resourceKey
    }

    @Test
    fun `fields are mapped correctly`() {
        everyRecordHeader(syncIndex = 0)

        val entity = createKafkaEntity()

        assertEquals(resourceName, entity.name)
        assertEquals(resourceKey, entity.key)
        assertEquals(resource, entity.resource)
        assertEquals(lastModified, entity.lastModified)
        assertEquals(syncCorrId, entity.syncCorrId)
        assertEquals(syncTotalSize, entity.syncTotalSize)
    }

    @Test
    fun `fullSync type is set and converted`() {
        everyRecordHeader(syncIndex = 0)

        val entity = createKafkaEntity()

        assertEquals(SyncType.FULL, entity.syncType)
    }

    @Test
    fun `deltaSync type is set and converted`() {
        everyRecordHeader(syncIndex = 1)

        val entity = createKafkaEntity()

        assertEquals(SyncType.DELTA, entity.syncType)
    }

    @Test
    fun `deleteSync type is set and converted`() {
        everyRecordHeader(syncIndex = 2)

        val entity = createKafkaEntity()

        assertEquals(SyncType.DELETE, entity.syncType)
    }

    @Test
    fun `unknown syncType index throws IllegalArgumentException`() {
        everyRecordHeader(syncIndex = 127)

        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    @Test
    fun `missing syncType throws IllegalArgumentException`() {
        everyRecordHeader(excludedHeader = SYNC_TYPE)

        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    @Test
    fun `negative syncType index throws IllegalArgumentException`() {
        everyRecordHeader(syncIndex = (-1).toByte())
        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    @Test
    fun `missing lastModified header throws IllegalArgumentException`() {
        everyRecordHeader(excludedHeader = LAST_MODIFIED)

        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    @Test
    fun `missing syncCorrId header throws IllegalArgumentException`() {
        everyRecordHeader(excludedHeader = SYNC_CORRELATION_ID)

        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    @Test
    fun `missing syncTotalSize header throws IllegalArgumentException`() {
        everyRecordHeader(excludedHeader = SYNC_TOTAL_SIZE)

        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    private fun everyRecordHeader(excludedHeader: String? = null, syncIndex: Byte = 0) =
        every { consumerRecord.headers() } returns
                RecordHeaders().apply {
                    add(SYNC_TYPE, byteArrayOf(syncIndex))
                    add(LAST_MODIFIED, currentTimeByteArray)
                    add(SYNC_CORRELATION_ID, syncCorrIdByteArray)
                    add(SYNC_TOTAL_SIZE, totalSizeByteArray)
                }.also { header -> excludedHeader?.let { header.remove(it) } }

    private fun createKafkaEntity() =
        ResourceKafkaEntity.from(
            resourceName = resourceName,
            resource = resource,
            record = consumerRecord
        )

}