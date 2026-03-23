package no.fintlabs.consumer.health

import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit

interface EndOffsetProvider {
    fun latestOffsets(partitions: Set<TopicPartition>): Map<TopicPartition, Long>
}

@Component
class KafkaAdminEndOffsetProvider(
    kafkaProperties: KafkaProperties,
) : EndOffsetProvider {
    private val adminClient = AdminClient.create(kafkaProperties.buildAdminProperties(null))

    override fun latestOffsets(partitions: Set<TopicPartition>): Map<TopicPartition, Long> {
        if (partitions.isEmpty()) {
            return emptyMap()
        }

        return adminClient
            .listOffsets(partitions.associateWith { OffsetSpec.latest() })
            .all()
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)
            .mapValues { (_, result) -> result.offset() }
    }

    @PreDestroy
    fun close() {
        adminClient.close(TIMEOUT)
    }

    companion object {
        private val TIMEOUT = Duration.ofSeconds(10)
    }
}
