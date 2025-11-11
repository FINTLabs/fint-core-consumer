package no.fintlabs.consumer.kafka

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers

internal fun Headers.require(name: String): Header = lastHeader(name) ?: throw IllegalArgumentException("Missing header: $name")

internal fun Headers.byte(name: String): Byte = KafkaHeader.getByte(require(name))

internal fun Headers.long(name: String): Long = KafkaHeader.getLong(require(name))

internal fun Headers.nullableLong(name: String): Long? = lastHeader(name)?.let { KafkaHeader.getLong(it) }

internal fun Headers.string(name: String): String = KafkaHeader.getString(require(name))
