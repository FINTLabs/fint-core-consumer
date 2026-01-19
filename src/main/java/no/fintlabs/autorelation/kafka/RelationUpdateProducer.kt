package no.fintlabs.autorelation.kafka

import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.kafka.entity.EntityProducerFactory
import no.fintlabs.kafka.entity.EntityProducerRecord
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters
import no.fintlabs.kafka.entity.topic.EntityTopicService
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.CompletableFuture

@Component
class RelationUpdateProducer(
    entityTopicService: EntityTopicService,
    entityProducerFactory: EntityProducerFactory,
    private val consumerConfiguration: ConsumerConfiguration,
) {
    companion object {
        /**
         * Retention time (7 days) matches the Core 2 maximum to ensure
         * relation updates do not expire before their associated resources.
         */
        const val RETENTION_TIME_IN_DAYS = 7L
    }

    private val entityTopic = createEntityTopic()
    private val entityProducer = entityProducerFactory.createProducer(RelationUpdate::class.java)

    init {
        entityTopicService.ensureTopic(entityTopic, Duration.ofDays(RETENTION_TIME_IN_DAYS).toMillis())
    }

    fun publishRelationUpdate(relationUpdate: RelationUpdate): CompletableFuture<SendResult<String, RelationUpdate>> =
        entityProducer.send(
            EntityProducerRecord
                .builder<RelationUpdate>()
                .topicNameParameters(entityTopic)
                .key(relationUpdate.targetId)
                .value(relationUpdate)
                .build(),
        )

    private fun createEntityTopic() =
        EntityTopicNameParameters
            .builder()
            .orgId(consumerConfiguration.orgId.toTopicFormat())
            .domainContext("fint-core")
            .resource("relation-update")
            .build()

    private fun String.toTopicFormat() = replace(".", "-")
}
