package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_CORRELATION_ID
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TOTAL_SIZE
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TYPE
import no.fintlabs.consumer.resource.ResourceConverter
import no.fintlabs.kafka.RESOURCE_KEY_DELIMITER
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.Optional
import java.util.UUID

class ResourceMessageTest {
    private val resourceConverter = mockk<ResourceConverter>()
    private val convertedResource = mockk<FintResource>()

    @BeforeEach
    fun setUp() {
        every { resourceConverter.convert(any(), any()) } returns convertedResource
    }

    @Test
    fun `fields are mapped correctly`() {
        val key = "elevfravar${RESOURCE_KEY_DELIMITER}test-key"
        val resourceName = "elevfravar"
        val resource = ElevfravarResource()
        val timestamp = 123456789L
        val corrId = UUID.randomUUID().toString()
        val totalSize = 100L

        val record =
            createRecord(
                key = key,
                value = resource,
                timestamp = timestamp,
                resourceName = resourceName,
                syncTypeOrdinal = SyncType.DELTA.ordinal,
                syncCorrelationId = corrId,
                syncTotalSize = totalSize,
            )

        val message = record.toResourceMessage(resourceConverter)

        assertEquals(resourceName, message.resourceName)
        assertEquals("test-key", message.resourceId)
        assertEquals(convertedResource, message.resource)
        assertEquals(timestamp, message.timestamp)
        assertEquals(corrId, message.syncMetadata?.corrId)
        assertEquals(SyncType.DELTA, message.syncMetadata?.syncType)
        assertEquals(totalSize, message.syncMetadata?.totalSize)
    }

    @Test
    fun `resourceId uses full key when no delimiter present`() {
        val record =
            createRecord(
                key = "plain-key",
                resourceName = "elevfravar",
            )

        val message = record.toResourceMessage(resourceConverter)

        assertEquals("plain-key", message.resourceId)
    }

    @Test
    fun `resourceId strips prefix when delimiter present`() {
        val record =
            createRecord(
                key = "elevfravar${RESOURCE_KEY_DELIMITER}abc-123",
                resourceName = "elevfravar",
            )

        val message = record.toResourceMessage(resourceConverter)

        assertEquals("abc-123", message.resourceId)
    }

    @Test
    fun `null value produces null resource`() {
        val record =
            createRecord(
                key = "key",
                value = null,
                resourceName = "elevfravar",
            )

        val message = record.toResourceMessage(resourceConverter)

        assertNull(message.resource)
    }

    @Test
    fun `missing RESOURCE_NAME header throws`() {
        val record =
            createRecord(
                key = "key",
                resourceName = null,
            )

        assertThrows(IllegalArgumentException::class.java) {
            record.toResourceMessage(resourceConverter)
        }
    }

    @Test
    fun `missing LAST_MODIFIED header throws`() {
        val record =
            createRecord(
                key = "key",
                resourceName = "elevfravar",
                includeLastModified = false,
            )

        assertThrows(IllegalArgumentException::class.java) {
            record.toResourceMessage(resourceConverter)
        }
    }

    @Test
    fun `syncMetadata is null when SYNC_TYPE header is absent`() {
        val record =
            createRecord(
                key = "key",
                resourceName = "elevfravar",
                syncTypeOrdinal = null,
            )

        val message = record.toResourceMessage(resourceConverter)

        assertNull(message.syncMetadata)
    }

    @Test
    fun `fullSync type is mapped correctly`() {
        val record =
            createRecord(
                key = "key",
                resourceName = "elevfravar",
                syncTypeOrdinal = SyncType.FULL.ordinal,
                syncCorrelationId = "corr-id",
                syncTotalSize = 10L,
            )

        val message = record.toResourceMessage(resourceConverter)

        assertEquals(SyncType.FULL, message.syncMetadata?.syncType)
    }

    @Test
    fun `deleteSync type is mapped correctly`() {
        val record =
            createRecord(
                key = "key",
                resourceName = "elevfravar",
                syncTypeOrdinal = SyncType.DELETE.ordinal,
                syncCorrelationId = "corr-id",
                syncTotalSize = 10L,
            )

        val message = record.toResourceMessage(resourceConverter)

        assertEquals(SyncType.DELETE, message.syncMetadata?.syncType)
    }

    @Test
    fun `unknown syncType throws`() {
        val record =
            createRecord(
                key = "key",
                resourceName = "elevfravar",
                syncTypeOrdinal = 123,
                syncCorrelationId = "corr-id",
                syncTotalSize = 10L,
            )

        assertThrows(IllegalArgumentException::class.java) {
            record.toResourceMessage(resourceConverter)
        }
    }

    private fun createRecord(
        key: String = UUID.randomUUID().toString(),
        value: Any? = ElevfravarResource(),
        timestamp: Long = 0L,
        resourceName: String?,
        includeLastModified: Boolean = true,
        syncTypeOrdinal: Int? = null,
        syncCorrelationId: String? = null,
        syncTotalSize: Long? = null,
    ): ConsumerRecord<String, Any?> {
        val headers = RecordHeaders()
        if (includeLastModified) {
            headers.add(
                RecordHeader(
                    LAST_MODIFIED,
                    ByteBuffer.allocate(Long.SIZE_BYTES).putLong(timestamp).array(),
                ),
            )
        }
        if (resourceName != null) {
            headers.add(RecordHeader(RESOURCE_NAME, resourceName.toByteArray()))
        }
        if (syncTypeOrdinal != null) {
            headers.add(RecordHeader(SYNC_TYPE, byteArrayOf(syncTypeOrdinal.toByte())))
        }
        if (syncCorrelationId != null) {
            headers.add(RecordHeader(SYNC_CORRELATION_ID, syncCorrelationId.toByteArray()))
        }
        if (syncTotalSize != null) {
            headers.add(
                RecordHeader(
                    SYNC_TOTAL_SIZE,
                    ByteBuffer.allocate(Long.SIZE_BYTES).putLong(syncTotalSize).array(),
                ),
            )
        }
        return ConsumerRecord(
            "test-topic",
            0,
            0,
            timestamp,
            TimestampType.CREATE_TIME,
            NULL_SIZE,
            NULL_SIZE,
            key,
            value,
            headers,
            Optional.empty<Int>(),
        )
    }
}
