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

    @BeforeEach
    fun setUp() {
        every { consumerRecord.key() } returns resourceKey
    }

    @Test
    fun `fields are mapped correctly`() {
        val syncType = SyncType.FULL

        everyRecordHeader(syncType = syncType)

        val entity = createKafkaEntity()

        assertEquals(resourceName, entity.resourceName)
        assertEquals(resourceKey, entity.key)
        assertEquals(resource, entity.resource)
        assertEquals(lastModified, entity.lastModified)
        assertEquals(syncCorrId, entity.consumerRecordMetadata?.corrId)
        assertEquals(syncTotalSize, entity.consumerRecordMetadata?.totalSize)
        assertEquals(syncType, entity.consumerRecordMetadata?.type)
    }

    @Test
    fun `fullSync type is set and converted`() {
        everyRecordHeader(syncType = SyncType.FULL)

        val entity = createKafkaEntity()

        assertEquals(SyncType.FULL, entity.consumerRecordMetadata?.type)
    }

    @Test
    fun `deltaSync type is set and converted`() {
        everyRecordHeader(syncType = SyncType.DELTA)

        val entity = createKafkaEntity()

        assertEquals(SyncType.DELTA, entity.consumerRecordMetadata?.type)
    }

    @Test
    fun `deleteSync type is set and converted`() {
        everyRecordHeader(syncType = SyncType.DELETE)

        val entity = createKafkaEntity()

        assertEquals(SyncType.DELETE, entity.consumerRecordMetadata?.type)
    }

    @Test
    fun `unknown syncType ordinal throws IllegalArgumentException`() {
        everyRecordHeader(syncTypeOrdinal = 127)

        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    @Test
    fun `missing syncType throws IllegalArgumentException`() {
        everyRecordHeader(excludedHeader = SYNC_TYPE)

        assertThrows(IllegalArgumentException::class.java) { createKafkaEntity() }
    }

    @Test
    fun `negative syncType index throws IllegalArgumentException`() {
        everyRecordHeader(syncTypeOrdinal = -5)
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

    private fun everyRecordHeader(excludedHeader: String? = null) = everyRecordHeader(excludedHeader, syncTypeOrdinal = 0)

    private fun everyRecordHeader(
        excludedHeader: String? = null,
        syncType: SyncType = SyncType.FULL,
    ) = everyRecordHeader(excludedHeader, syncType.ordinal)

    private fun everyRecordHeader(
        excludedHeader: String? = null,
        syncTypeOrdinal: Int = 0,
    ) = every { consumerRecord.headers() } returns
        RecordHeaders()
            .apply {
                add(SYNC_TYPE, byteArrayOf(syncTypeOrdinal.toByte()))
                add(LAST_MODIFIED, currentTimeByteArray)
                add(SYNC_CORRELATION_ID, syncCorrIdByteArray)
                add(SYNC_TOTAL_SIZE, totalSizeByteArray)
            }.also { header -> excludedHeader?.let { header.remove(it) } }

    private fun createKafkaEntity() =
        createKafkaEntity(
            resourceName = resourceName,
            resource = resource,
            record = consumerRecord,
        )
}
