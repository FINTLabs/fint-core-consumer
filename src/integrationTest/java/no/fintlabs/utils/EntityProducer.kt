package no.fintlabs.utils

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.RESOURCE_NAME
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_CORRELATION_ID
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TOTAL_SIZE
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TYPE
import no.fintlabs.kafka.KafkaTopicName
import no.fintlabs.kafka.RESOURCE_KEY_DELIMITER
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.springframework.boot.test.context.TestComponent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

@TestComponent
class EntityProducer(
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    private val consumerConfig: ConsumerConfiguration,
) {
    fun produce(
        resourceName: String,
        resource: Any?,
        resourceId: String,
        syncType: SyncType,
        syncCorrId: String,
        syncTotalSize: Long,
        timestamp: Long = System.currentTimeMillis(),
        domainName: String = consumerConfig.domain,
        packageName: String = consumerConfig.packageName,
    ): CompletableFuture<SendResult<String, Any>> {
        val topic =
            KafkaTopicName.entity(
                orgId = consumerConfig.orgId,
                resourceName = "$domainName-$packageName",
            )
        val key = "$resourceName$RESOURCE_KEY_DELIMITER$resourceId"
        val headers = createSyncHeaders(resourceName, syncType, syncCorrId, syncTotalSize, timestamp)

        val record = ProducerRecord<String, Any>(topic, null, null, key, resource, headers)
        return kafkaTemplate.send(record)
    }

    fun produceToLegacyResourceTopic(
        resourceName: String,
        resource: Any?,
        resourceId: String,
        syncType: SyncType,
        syncCorrId: String,
        syncTotalSize: Long,
        timestamp: Long = System.currentTimeMillis(),
        includeResourceNameHeader: Boolean = true,
        domainName: String = consumerConfig.domain,
        packageName: String = consumerConfig.packageName,
    ): CompletableFuture<SendResult<String, Any>> {
        val topic =
            KafkaTopicName.entity(
                orgId = consumerConfig.orgId,
                resourceName = "$domainName-$packageName-$resourceName",
            )
        val key = "$resourceName$RESOURCE_KEY_DELIMITER$resourceId"
        val headers =
            createSyncHeaders(
                if (includeResourceNameHeader) resourceName else null,
                syncType,
                syncCorrId,
                syncTotalSize,
                timestamp,
            )

        val record = ProducerRecord<String, Any>(topic, null, null, key, resource, headers)
        return kafkaTemplate.send(record)
    }

    private fun createSyncHeaders(
        resourceName: String?,
        syncType: SyncType,
        syncCorrId: String,
        syncTotalSize: Long,
        timestamp: Long,
    ) = RecordHeaders().apply {
        if (resourceName != null) add(RecordHeader(RESOURCE_NAME, resourceName.toByteArray()))
        add(SYNC_TYPE, byteArrayOf(syncType.ordinal.toByte()))
        add(SYNC_CORRELATION_ID, syncCorrId.toByteArray())
        add(SYNC_TOTAL_SIZE, ByteBuffer.allocate(Long.SIZE_BYTES).putLong(syncTotalSize).array())
        add(LAST_MODIFIED, ByteBuffer.allocate(Long.SIZE_BYTES).putLong(timestamp).array())
    }
}
