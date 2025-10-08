package no.fintlabs.consumer.kafka.entity

import io.mockk.every
import io.mockk.mockk
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.KafkaConstants.MODIFIED_TIME
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TYPE
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

    @BeforeEach
    fun setUp() {
        every { consumerRecord.key() } returns (UUID.randomUUID().toString())
    }

    @Test
    fun `fullSync type is set and converted`() {
        val syncIndex = 0

        every { consumerRecord.headers() } returns (
                RecordHeaders().apply {
                    add(SYNC_TYPE, byteArrayOf(syncIndex.toByte()))
                    add(MODIFIED_TIME, currentTimeByteArray)
                }
                )

        val entity = ResourceKafkaEntity.from(
            resourceName = "elevfravar",
            resource = ElevfravarResource(),
            record = consumerRecord
        )

        assertEquals(SyncType.FULL, entity.syncType)
    }

    @Test
    fun `deltaSync type is set and converted`() {
        val syncIndex = 1

        every { consumerRecord.headers() } returns (
                RecordHeaders().apply {
                    add(SYNC_TYPE, byteArrayOf(syncIndex.toByte()))
                    add(MODIFIED_TIME, currentTimeByteArray)
                }
                )

        val entity = ResourceKafkaEntity.from(
            resourceName = "elevfravar",
            resource = ElevfravarResource(),
            record = consumerRecord
        )

        assertEquals(SyncType.DELTA, entity.syncType)
    }

    @Test
    fun `deleteSync type is set and converted`() {
        val syncIndex = 2

        every { consumerRecord.headers() } returns (
                RecordHeaders().apply {
                    add(SYNC_TYPE, byteArrayOf(syncIndex.toByte()))
                    add(MODIFIED_TIME, currentTimeByteArray)
                }
                )

        val entity = ResourceKafkaEntity.from(
            resourceName = "elevfravar",
            resource = ElevfravarResource(),
            record = consumerRecord
        )

        assertEquals(SyncType.DELETE, entity.syncType)
    }

    @Test
    fun `missing syncType throws IllegalArgumentException`() {
        every { consumerRecord.headers() } returns (
                RecordHeaders().apply { add(MODIFIED_TIME, currentTimeByteArray) }
        )

        assertThrows(IllegalArgumentException::class.java) {
            ResourceKafkaEntity.from(
                resourceName = "elevfravar",
                resource = ElevfravarResource(),
                record = consumerRecord
            )
        }
    }


}