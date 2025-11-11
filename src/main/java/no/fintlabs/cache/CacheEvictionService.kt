package no.fintlabs.cache

import no.fintlabs.autorelation.model.createDeleteRequest
import no.fintlabs.cache.config.EvictionConfig
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationRequestProducer
import no.fintlabs.status.models.ResourceEvictionPayload
import org.springframework.scheduling.TaskScheduler
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Duration
import java.time.Instant

@Service
class CacheEvictionService(
    private val scheduler: TaskScheduler,
    private val cacheService: CacheService,
    private val evictionConfig: EvictionConfig,
    private val clock: Clock = Clock.systemUTC(),
    private val consumerConfig: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer
) {

    fun triggerEviction(resourceEvictionPayload: ResourceEvictionPayload) =
        resourceEvictionPayload.takeIf { requestIsWithinDeterminedTime(it) }
            ?.let { scheduleEviction(it) }

    private fun scheduleEviction(resourceEvictionPayload: ResourceEvictionPayload) =
        scheduler.schedule(
            { processEviction(resourceEvictionPayload) },
            clock.instant().plus(evictionConfig.evictionDelay)
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
            createDeleteRequest(
                consumerConfig.orgId,
                consumerConfig.domain,
                consumerConfig.packageName,
                resource,
                resourceObject
            )
        )

    private fun requestIsWithinDeterminedTime(payload: ResourceEvictionPayload): Boolean {
        val now = clock.instant()
        val timestamp = Instant.ofEpochMilli(payload.unixTimestamp)
        if (timestamp.isAfter(now)) {
            return false
        }
        return Duration.between(timestamp, now) <= evictionConfig.acceptanceWindow
    }
}
