package no.fintlabs.consumer.kafka.entity

import java.nio.charset.StandardCharsets
import java.time.Duration
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_CORRELATION_ID
import no.fintlabs.consumer.resource.ResourceMapperService
import no.fintlabs.consumer.resource.ResourceService
import no.fintlabs.kafka.common.topic.pattern.FormattedTopicComponentPattern
import no.fintlabs.kafka.entity.EntityConsumerFactoryService
import no.fintlabs.kafka.entity.topic.EntityTopicNamePatternParameters
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Service

@Service
class EntityConsumer(
    private val resourceService: ResourceService,
    private val consumerConfig: ConsumerConfiguration,
    private val resourceMapper: ResourceMapperService,
) {
    @Bean
    fun resourceEntityConsumerFactory(consumerFactoryService: EntityConsumerFactoryService): ConcurrentMessageListenerContainer<String?, in Any>? =
        consumerFactoryService
            .createFactory(Any::class.java, this::consumeRecord)
            .createContainer(
                EntityTopicNamePatternParameters
                    .builder()
                    .orgId(FormattedTopicComponentPattern.anyOf(createOrgId()))
                    .domainContext(FormattedTopicComponentPattern.anyOf("fint-core"))
                    .resource(FormattedTopicComponentPattern.startingWith(createResourcePattern()))
                    .build(),
            ) // TODO: Upgrade to fint-kafka 5 - skip failed messages & commit them onto a DLQ

    fun consumeRecord(consumerRecord: ConsumerRecord<String, Any>) {
        val startNanos = System.nanoTime()
        var failed = false

        try {
            createKafkaEntity(consumerRecord).let { resourceService.processEntityConsumerRecord(it) }
        } catch (exception: Exception) {
            failed = true
            throw exception
        } finally {
            val duration = Duration.ofNanos(System.nanoTime() - startNanos)
            if (duration > SLOW_PROCESSING_THRESHOLD) {
                logger.warn(
                    "Slow Kafka message processing: durationMs={}, topic={}, partition={}, offset={}, key={}, corrId={}, failed={}",
                    duration.toMillis(),
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.key(),
                    consumerRecord.correlationId(),
                    failed,
                )
            }
        }
    }

    private fun createKafkaEntity(consumerRecord: ConsumerRecord<String, Any>) =
        getResourceName(consumerRecord.topic()).let { resourceName ->
            resourceMapper
                .mapResource(resourceName, consumerRecord.value())
                .let { resource -> createKafkaEntity(resourceName, resource, consumerRecord) }
        }

    private fun createOrgId() = consumerConfig.orgId.replace(".", "-")

    private fun createResourcePattern() = "${consumerConfig.domain}-${consumerConfig.packageName}"

    private fun getResourceName(topic: String) = topic.substringAfterLast("-")

    private fun ConsumerRecord<String, *>.correlationId(): String? =
        headers()
            .lastHeader(SYNC_CORRELATION_ID)
            ?.value()
            ?.toString(StandardCharsets.UTF_8)

    companion object {
        private val logger = LoggerFactory.getLogger(EntityConsumer::class.java)
        private val SLOW_PROCESSING_THRESHOLD: Duration = Duration.ofSeconds(10)
    }
}
