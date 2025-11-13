package no.fintlabs.consumer.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

internal fun ConsumerRecord<String, *>.headerByteValue(name: String): Byte? {
    val value = headers().lastHeader(name)?.value() ?: return null

    if (value.size != 1) {
        throw IllegalArgumentException("Header '$name' contains ${value.size} bytes, but expected 1")
    }

    return value[0]
}

internal fun ConsumerRecord<String, *>.headerStringValue(name: String): String? =
    headers().lastHeader(name)?.value()?.toString(StandardCharsets.UTF_8)

internal fun ConsumerRecord<String, *>.headerLongValue(name: String): Long? =
    headers().lastHeader(name)?.value()?.let {
        if (it.size >= 8) ByteBuffer.wrap(it).long else null
    }
