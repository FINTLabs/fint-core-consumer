package no.fintlabs.consumer.health

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit

interface EndOffsetProvider {
    fun latestOffsets(partitions: Set<TopicPartition>): Map<TopicPartition, Long>
}

@Component
class KafkaAdminEndOffsetProvider(
    private val adminClient: AdminClient,
) : EndOffsetProvider {
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

    companion object {
        private val TIMEOUT = Duration.ofSeconds(10)
    }
}
