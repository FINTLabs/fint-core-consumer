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
    fun triggerEviction(resource: String) =
        cacheService
            .getCache(resource)
            ?.let { cache ->
                cache.evictOldCacheObjects { _, cacheObject ->
                    onCacheEviction(resource, cacheObject.unboxObject())
                }
            }

    private fun onCacheEviction(
        resource: String,
        resourceObject: Any,
    ) = relationRequestProducer.publish(
        createDeleteRequest(
            consumerConfig.orgId,
            consumerConfig.domain,
            consumerConfig.packageName,
            resource,
            resourceObject,
        ),
    )
}
