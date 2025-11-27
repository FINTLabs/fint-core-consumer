package no.fintlabs.consumer.kafka

import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class ConsumerRecordExtensionTest {
    @Test
    fun `headerByteValue returns header byte value when the header exist and contains only a single byte`() {
        // Given
        val record = mockConsumerRecordWithHeader<String, String>("byteHeader", byteArrayOf(42))

        // When
        val headerByteValue = record.headerByteValue("byteHeader")

        // Then
        assertEquals(42.toByte(), headerByteValue)
    }

    @Test
    fun `headerByteValue returns header byte value when the header exist and is an int`() {
        // Given
        val int = 5

        val intByteArray =
            ByteBuffer
                .allocate(Int.SIZE_BYTES)
                .putInt(int)
                .array()

        val record = mockConsumerRecordWithHeader<String, String>("byteHeader", intByteArray)

        // When
        val headerByteValue = record.headerByteValue("byteHeader")

        // Then
        assertEquals(int.toByte(), headerByteValue)
    }

    @Test
    fun `headerByteValue returns null when the header does not exist`() {
        // Given
        val record = mockConsumerRecordWithoutHeaders<String, String>()

        // When
        val headerByteValue = record.headerByteValue("byteHeader")

        // Then
        assertNull(headerByteValue)
    }

    @Test
    fun `headerByteValue throws exception when the header exist and contains multiple bytes`() {
        // Given
        val record = mockConsumerRecordWithHeader<String, String>("byteHeader", byteArrayOf(42, 10))

        // Then
        assertThrows<IllegalArgumentException> { record.headerByteValue("byteHeader") }
    }

    @Test
    fun headerStringValue() {
        // Given
        val stringValue = "KafkaTest"
        val stringAsByteArray = stringValue.toByteArray(StandardCharsets.UTF_8)
        val record = mockConsumerRecordWithHeader<String, String>("stringHeader", stringAsByteArray)

        // When
        val headerStringValue = record.headerStringValue("stringHeader")

        // Then
        assertEquals(stringValue, headerStringValue)
    }

    @Test
    fun headerLongValue() {
        // Given
        val longValue = 123456789L
        val longAsByteArray = ByteBuffer.allocate(8).putLong(longValue).array()
        val record = mockConsumerRecordWithHeader<String, String>("longHeader", longAsByteArray)

        // When
        val headerLongValue = record.headerLongValue("longHeader")

        // Then
        assertEquals(longValue, headerLongValue)
    }

    private fun <K, V> mockConsumerRecordWithHeader(
        key: String,
        headerValue: ByteArray,
    ): ConsumerRecord<K, V> {
        val header = mockk<Header>()
        every { header.value() } returns headerValue

        val headers = mockk<Headers>()
        every { headers.lastHeader(key) } returns header

        val record = mockk<ConsumerRecord<K, V>>()
        every { record.headers() } returns headers

        return record
    }

    private fun <K, V> mockConsumerRecordWithoutHeaders(): ConsumerRecord<K, V> {
        val headers = mockk<Headers>()
        every { headers.lastHeader(any()) } returns null

        val record = mockk<ConsumerRecord<K, V>>()
        every { record.headers() } returns headers

        return record
    }
}
