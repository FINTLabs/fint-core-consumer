package no.fintlabs.consumer.kafka

import org.apache.kafka.common.header.Header
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object KafkaHeader {

    fun getByte(header: Header): Byte = header.value()[0]

    fun getLong(header: Header): Long =
        ByteBuffer.wrap(header.value()).getLong()

    fun getInt(header: Header): Int =
        ByteBuffer.wrap(header.value()).getInt()

    fun getShort(header: Header): Short =
        ByteBuffer.wrap(header.value()).getShort()

    fun getString(header: Header): String =
        String(header.value(), StandardCharsets.UTF_8)

}
