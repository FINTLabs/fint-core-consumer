package no.fintlabs.consumer.kafka.entity

import io.mockk.mockk
import no.novari.fint.model.resource.FintResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows
import java.lang.Math.random
import java.nio.ByteBuffer
import java.util.*
import kotlin.test.assertEquals

class KafkaEntityTest {
    private val resourceName = "elevfravar"
    private val resource = mockk<FintResource>()

    /**
     * Sync record is a record related to a sync. We know it's related to a sync if it contains a SyncType header.
     */
    @Test
    fun `createKafkaEntity should successfully create a KafkaEntity on Sync Record`() {
        val key = UUID.randomUUID().toString()
        val lastModified = random().toLong()
        val retentionTime = random().toLong()
        val syncTypeByte: Byte = SyncType.FULL.ordinal.toByte()
        val syncCorrId = UUID.randomUUID().toString()
        val syncTotalSize = random().toLong()

        val record =
            createConsumerRecord(
                key = key,
                recordHeaders =
                    createRecordHeaders(
                        lastModified = lastModified,
                        retentionTime = retentionTime,
                        syncTypeByte = syncTypeByte,
                        syncCorrId = syncCorrId,
                        syncTotalSize = syncTotalSize,
                    ),
            )

        val entity = createKafkaEntity(resourceName, resource, record)

        assertEquals(key, entity.key)
        assertEquals(lastModified, entity.lastModified)
        assertEquals(retentionTime, entity.retentionTime)

        assertNotNull(entity.consumerRecordMetadata)
        assertEquals(SyncType.FULL, entity.consumerRecordMetadata.type)
        assertEquals(syncCorrId, entity.consumerRecordMetadata.corrId)
        assertEquals(syncTotalSize, entity.consumerRecordMetadata.totalSize)
    }

    @Test
    fun `createKafkaEntity should throw exception when Kafka Record Key is missing`() {
        val record =
            createConsumerRecord(
                key = null,
                recordHeaders = createRecordHeaders(lastModified = random().toLong()),
            )

        val exception =
            assertThrows<IllegalArgumentException> {
                createKafkaEntity(resourceName, resource, record)
            }

        assertEquals("Key is missing", exception.message)
    }

    @Test
    fun `createKafkaEntity should throw exception when LastModified header is missing`() {
        val record =
            createConsumerRecord(
                key = UUID.randomUUID().toString(),
                recordHeaders =
                    createRecordHeaders(
                        lastModified = null,
                    ),
            )

        val exception =
            assertThrows<IllegalArgumentException> {
                createKafkaEntity(resourceName, resource, record)
            }

        assertEquals("Last modified timestamp is missing", exception.message)
    }

    @Test
    fun `createKafkaEntity should throw exception when SyncType is present but CorrId is missing`() {
        val record =
            createConsumerRecord(
                key = UUID.randomUUID().toString(),
                recordHeaders =
                    createRecordHeaders(
                        lastModified = random().toLong(),
                        syncTypeByte = SyncType.FULL.ordinal.toByte(),
                        syncCorrId = null,
                    ),
            )

        val exception =
            assertThrows<IllegalArgumentException> {
                createKafkaEntity(resourceName, resource, record)
            }

        assertEquals("corrId cannot be null", exception.message)
    }

    @Test
    fun `createKafkaEntity should throw exception when SyncType is present but CorrId is blank`() {
        val record =
            createConsumerRecord(
                key = UUID.randomUUID().toString(),
                recordHeaders =
                    createRecordHeaders(
                        lastModified = random().toLong(),
                        syncTypeByte = SyncType.FULL.ordinal.toByte(),
                        syncCorrId = "   ",
                    ),
            )

        val exception =
            assertThrows<IllegalArgumentException> {
                createKafkaEntity(resourceName, resource, record)
            }

        assertEquals("corrId cannot be null", exception.message)
    }

    @Test
    fun `createKafkaEntity should throw exception when SyncType is present but TotalSize is missing`() {
        val record =
            createConsumerRecord(
                key = UUID.randomUUID().toString(),
                recordHeaders =
                    createRecordHeaders(
                        lastModified = random().toLong(),
                        syncTypeByte = SyncType.FULL.ordinal.toByte(),
                        syncCorrId = UUID.randomUUID().toString(),
                        syncTotalSize = null,
                    ),
            )

        val exception =
            assertThrows<IllegalArgumentException> {
                createKafkaEntity(resourceName, resource, record)
            }

        assertEquals("totalSize cannot be null", exception.message)
    }

    @Test
    fun `createKafkaEntity should throw exception when SyncType byte is invalid`() {
        val invalidOrdinal: Byte = 99
        val record =
            createConsumerRecord(
                key = UUID.randomUUID().toString(),
                recordHeaders =
                    createRecordHeaders(
                        lastModified = random().toLong(),
                        syncTypeByte = invalidOrdinal,
                    ),
            )

        val exception =
            assertThrows<IllegalArgumentException> {
                createKafkaEntity(resourceName, resource, record)
            }

        assertEquals("Invalid SyncType value: $invalidOrdinal", exception.message)
    }

    @Test
    fun `createKafkaEntity should succeed even if retentionTime header is missing`() {
        val key = UUID.randomUUID().toString()
        val lastModified = random().toLong()
        val syncTypeByte: Byte = SyncType.FULL.ordinal.toByte()
        val syncCorrId = UUID.randomUUID().toString()
        val syncTotalSize = random().toLong()

        val record =
            createConsumerRecord(
                key = key,
                recordHeaders =
                    createRecordHeaders(
                        lastModified = lastModified,
                        syncTypeByte = syncTypeByte,
                        syncCorrId = syncCorrId,
                        syncTotalSize = syncTotalSize,
                    ),
            )

        val entity = createKafkaEntity(resourceName, resource, record)

        assertEquals(key, entity.key)
        assertEquals(lastModified, entity.lastModified)
        assertNull(entity.retentionTime)

        assertNotNull(entity.consumerRecordMetadata)
        assertEquals(SyncType.FULL, entity.consumerRecordMetadata.type)
        assertEquals(syncCorrId, entity.consumerRecordMetadata.corrId)
        assertEquals(syncTotalSize, entity.consumerRecordMetadata.totalSize)
    }

    /**
     * Event record is a record related to a Fint Event. We know it's related to an event if it's missing the SyncType header.
     */
    @Test
    fun `createKafkaEntity should not create consumerRecordMetadata on Event Record`() {
        val key = UUID.randomUUID().toString()
        val lastModified = random().toLong()
        val retentionTime = random().toLong()
        val syncTypeByte = null
        val syncCorrId = UUID.randomUUID().toString()
        val syncTotalSize = random().toLong()

        val record =
            createConsumerRecord(
                key = key,
                recordHeaders =
                    createRecordHeaders(
                        lastModified = lastModified,
                        retentionTime = retentionTime,
                        syncTypeByte = syncTypeByte,
                        syncCorrId = syncCorrId,
                        syncTotalSize = syncTotalSize,
                    ),
            )

        val entity = createKafkaEntity(resourceName, resource, record)

        assertEquals(key, entity.key)
        assertEquals(lastModified, entity.lastModified)
        assertEquals(retentionTime, entity.retentionTime)

        assertNull(entity.consumerRecordMetadata)
    }

    private fun createConsumerRecord(
        key: String?,
        recordHeaders: RecordHeaders = RecordHeaders(),
    ) = ConsumerRecord<String, Any>("unused here", 0, 0, key, "unused here")
        .apply { recordHeaders.forEach { headers().add(it) } }

    private fun createRecordHeaders(
        lastModified: Long? = null,
        retentionTime: Long? = null,
        syncTypeByte: Byte? = null, // Converted to enum based on enum ordinal e.g 0 = FULL
        syncCorrId: String? = null,
        syncTotalSize: Long? = null,
    ) = RecordHeaders().apply {
        addIfNotNull(LAST_MODIFIED, lastModified.toHeaderBytes())
        addIfNotNull(TOPIC_RETENTION_TIME, retentionTime.toHeaderBytes())
        addIfNotNull(SYNC_TYPE, syncTypeByte.toHeaderBytes())
        addIfNotNull(SYNC_CORRELATION_ID, syncCorrId.toHeaderBytes())
        addIfNotNull(SYNC_TOTAL_SIZE, syncTotalSize.toHeaderBytes())
    }

    private fun RecordHeaders.addIfNotNull(
        key: String,
        value: ByteArray?,
    ) {
        if (value != null) {
            add(RecordHeader(key, value))
        }
    }

    private fun Long?.toHeaderBytes(): ByteArray? =
        this?.let {
            ByteBuffer.allocate(Long.SIZE_BYTES).putLong(it).array()
        }

    private fun String?.toHeaderBytes(): ByteArray? = this?.toByteArray()

    private fun Byte?.toHeaderBytes(): ByteArray? = this?.let { byteArrayOf(it) }
}
