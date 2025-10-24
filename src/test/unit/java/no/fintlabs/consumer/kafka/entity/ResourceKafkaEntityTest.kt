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
    private val currentTimeByteArray =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(System.currentTimeMillis()).array()
    private val stringByteArray = "hello".toByteArray()
    private val totalSizeByteArray = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(100).array()

    @BeforeEach
    fun setUp() {
        every { consumerRecord.key() } returns (UUID.randomUUID().toString())
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
                    add(SYNC_CORRELATION_ID, stringByteArray)
                    add(SYNC_TOTAL_SIZE, totalSizeByteArray)
                }.also { header -> excludedHeader?.let { header.remove(it) } }

    private fun createKafkaEntity() =
        ResourceKafkaEntity.from(
            resourceName = "elevfravar",
            resource = ElevfravarResource(),
            record = consumerRecord
        )

}