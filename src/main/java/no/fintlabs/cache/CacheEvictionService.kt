package no.fintlabs.cache

import no.fintlabs.autorelation.model.RelationRequest
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import no.fintlabs.status.models.ResourceEvictionPayload
import org.springframework.scheduling.TaskScheduler
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant


@Service
class CacheEvictionService(
    private val scheduler: TaskScheduler,
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer
) {

    companion object {
        const val MINUTES_TO_WAIT_BEFORE_EVICTING: Long = 10L
        const val MINUTES_TO_ACCEPT_EVICTION: Long = 10L
    }

    fun triggerEviction(resourceEvictionPayload: ResourceEvictionPayload) =
        resourceEvictionPayload.takeIf { requestIsWithinDeterminedTime(it) }
            ?.let { scheduleEviction(it) }

    private fun scheduleEviction(resourceEvictionPayload: ResourceEvictionPayload) =
        scheduler.schedule(
            { processEviction(resourceEvictionPayload) },
            Instant.now().plus(Duration.ofMinutes(MINUTES_TO_WAIT_BEFORE_EVICTING))
        )

    private fun processEviction(resourceEvictionPayload: ResourceEvictionPayload) =
        cacheService.getCache(resourceEvictionPayload.resource)
            ?.let { cache ->
                cache.evictOldCacheObjects { _, cacheObject ->
                    onCacheEviction(resourceEvictionPayload.resource, cacheObject.unboxObject())
                }
            }

    private fun onCacheEviction(resource: String, resourceObject: Any) =
        relationRequestProducer.publish(
            RelationRequest.from(
                consumerConfig.orgId,
                consumerConfig.domain,
                consumerConfig.packageName,
                resource,
                resourceObject
            )
        )

    private fun requestIsWithinDeterminedTime(payload: ResourceEvictionPayload): Boolean =
        Instant.ofEpochMilli(payload.unixTimestamp)
            .takeIf { it.isBefore(Instant.now()) }
            ?.let { Duration.between(it, Instant.now()) <= Duration.ofMinutes(MINUTES_TO_ACCEPT_EVICTION) }
            ?: false

}