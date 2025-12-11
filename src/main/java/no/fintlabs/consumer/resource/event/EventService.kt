package no.fintlabs.consumer.resource.event

import com.github.benmanes.caffeine.cache.Cache
import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import no.fintlabs.adapter.models.event.ResponseFintEvent
import org.springframework.stereotype.Service

@Service
@Slf4j
@RequiredArgsConstructor
class EventService(
    private val requestFintCorrIds: Cache<String, String>,
    private val responseFintEvents: Cache<String, ResponseFintEvent>,
) {
    fun registerResponse(
        key: String,
        response: ResponseFintEvent,
    ) = responseFintEvents.put(key, response)

    fun registerRequest(key: String) = requestFintCorrIds.put(key, key)

    fun getResponse(corrId: String): ResponseFintEvent? = responseFintEvents.getIfPresent(corrId)

    fun requestExists(corrId: String): Boolean = requestFintCorrIds.getIfPresent(corrId) != null
}
