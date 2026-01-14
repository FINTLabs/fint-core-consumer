package no.fintlabs.cache

import no.fintlabs.autorelation.model.createDeleteEvent
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import org.springframework.stereotype.Service

@Service
class CacheEvictionService(
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer,
) {
    fun evictExpired(resourceName: String) =
        cacheService
            .getCache(resourceName)
            ?.let { cache ->
                cache.evictOldCacheObjects { resourceId, cacheObject ->
                    onCacheEviction(resourceName, resourceId, cacheObject.unboxObject())
                }
            }

    private fun onCacheEviction(
        resourceName: String,
        resourceId: String,
        resourceData: Any,
    ) = relationRequestProducer.publish(
        createDeleteEvent(
            domainName = consumerConfig.domain,
            packageName = consumerConfig.packageName,
            orgId = consumerConfig.orgId,
            resourceName = resourceName,
            resource = resourceData,
            resourceId = resourceId,
        ),
    )
}
