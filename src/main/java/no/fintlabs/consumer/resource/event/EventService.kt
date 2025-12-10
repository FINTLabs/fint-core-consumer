package no.fintlabs.consumer.resource.event

import com.github.benmanes.caffeine.cache.Cache
import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import no.fint.model.resource.FintResource
import no.fintlabs.adapter.models.event.ResponseFintEvent
import no.fintlabs.cache.CacheService
import org.springframework.stereotype.Service

@Service
@Slf4j
@RequiredArgsConstructor
class EventService(
    private val requestFintCorrIds: Cache<String, String>,
    private val responseFintEvents: Cache<String, ResponseFintEvent>,
    private val cacheService: CacheService,
) {
    fun registerResponse(
        key: String,
        response: ResponseFintEvent,
    ) = responseFintEvents.put(key, response)

    fun registerRequest(key: String) = requestFintCorrIds.put(key, key)

    fun getResponse(corrId: String): ResponseFintEvent? = responseFintEvents.getIfPresent(corrId)

    fun requestExists(corrId: String): Boolean = requestFintCorrIds.getIfPresent(corrId) != null

    fun getResource(
        resourceName: String,
        corrId: String,
    ): FintResource? =
        responseFintEvents
            .getIfPresent(corrId)
            ?.takeIf { it.matchesLastDelivered(resourceName) }
            ?.let { cacheService.getCache(resourceName).get(it.value.identifier) }

    /**
     * Checks if the existing resource in the cache has already been updated with this event.
     *
     * If the existing resource in cache `lastDelivered` matches `handledAt`, it means that
     * the Event has been fully processed. Since the `lastDelivered` timestamp matches
     * the event's timestamp, we can assume the cache is consistent with this event version.
     */
    private fun ResponseFintEvent.matchesLastDelivered(resourceName: String): Boolean =
        cacheService.getCache(resourceName).getLastDelivered(value.identifier) == handledAt
}
