package no.fintlabs.cache

import no.fintlabs.autorelation.model.createDeleteRequest
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import org.springframework.stereotype.Service

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer,
) {
    fun evictExpired(resourceName: String, startTimestamp: Long) =
        cacheService.getCache(resourceName)
            .evictExpired(startTimestamp)
            .forEach { publishRelationDeleteRequest(resourceName, it) }

    private fun publishRelationDeleteRequest(
        resourceName: String,
        resource: Any,
    ) = relationRequestProducer.publish(
        createDeleteRequest(
            consumerConfig.orgId,
            consumerConfig.domain,
            consumerConfig.packageName,
            resourceName,
            resource,
        ),
    )
}
