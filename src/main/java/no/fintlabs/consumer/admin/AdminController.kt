package no.fintlabs.consumer.admin

import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.EndpointsConstants
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.util.*

@RestController
@RequestMapping(EndpointsConstants.ADMIN)
class AdminController(
    private val cacheService: CacheService,
    private val configuration: ConsumerConfiguration
) {

    @GetMapping("/health")
    fun healthChecks(): ResponseEntity<*>? = null // TODO: Implement when status service is working!

    @GetMapping("/organisations")
    @Deprecated("")
    fun organisations(): MutableCollection<String> = ArrayList<String>()

    @Deprecated("")
    @GetMapping("/organisations/{orgId:.+}")
    fun getOrganization(@PathVariable orgId: String?): MutableCollection<String?> {
        return ArrayList<String?>()
    }

    @GetMapping("/assets")
    fun assets(): MutableCollection<String?> = HashSet<String?>(listOf(configuration.orgId))

    @GetMapping("/caches")
    @Deprecated("")
    fun caches(): MutableMap<String, Int> = HashMap<String, Int>()

    /**
     * Get status for all caches.
     *
     * @return an object where each key is a resource name and each value is an object containing
     * lastUpdated and size for the cache for that resource name
     */
    @GetMapping("/cache/status")
    fun cacheStatus(): Map<String, CacheEntry> = cacheService.getCachedResourceNames().associateWith { resourceName ->
        val cache = cacheService.getCache(resourceName)
        CacheEntry(Date(cache.lastUpdated), cache.size)
    }

    @PostMapping("/cache/rebuild", "/cache/rebuild/{model}")
    fun rebuildCache(
        @RequestHeader(name = "x-client") client: String?,
        @PathVariable(required = false) model: String?
    ) {
        // TODO: Yet to be implemented
    }
}
