package no.fintlabs.consumer.kafka.sync

import no.fintlabs.consumer.config.OrgId
import no.fintlabs.consumer.kafka.sync.model.SyncStatus
import no.fintlabs.kafka.KafkaTopicName
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture

@Component
class SyncStatusProducer(
    private val kafkaTemplate: KafkaTemplate<String, SyncStatus>,
) {
    private val topic =
        KafkaTopicName.event(
            orgId = FINTLABS_ORG_ID,
            eventName = "sync-status",
        )

    fun produce(syncStatus: SyncStatus): CompletableFuture<SendResult<String, SyncStatus>> =
        kafkaTemplate.send(topic, syncStatus)

    companion object {
        private val FINTLABS_ORG_ID = OrgId.from("fintlabs.no")
    }
}
