package no.fintlabs.cache

import io.mockk.Called
import io.mockk.clearAllMocks
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.RelationEventService
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class CacheEvictionServiceTest {
    private lateinit var cacheService: CacheService
    private lateinit var relationEventService: RelationEventService
    private lateinit var cacheEvictionService: CacheEvictionService

    @BeforeEach
    fun setUp() {
        cacheService = CacheService()
        relationEventService = mockk(relaxed = true)
        cacheEvictionService =
            CacheEvictionService(
                cacheService = cacheService,
                relationEventService = relationEventService,
            )
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Test
    fun `eviction on empty cache or with unknown resource name does not call relationEventService`() {
        val resourceName = "unknown-resource"
        cacheEvictionService.evictExpired(resourceName, Long.MAX_VALUE)

        verify { relationEventService wasNot Called }
    }

    @Test
    fun `calls removeRelations for every evicted object`() {
        val resourceName = "elevfravar"
        val key1 = "k1"
        val key2 = "k2"

        val cache = cacheService.getCache(resourceName)
        val resource1 = ElevfravarResource()
        val resource2 = ElevfravarResource()
        cache.put(key1, resource1, 1)
        cache.put(key2, resource2, 2)
        cacheEvictionService.evictExpired(resourceName, Long.MAX_VALUE)

        verify(exactly = 1) {
            relationEventService.removeRelations(resourceName, key1, resource1)
            relationEventService.removeRelations(resourceName, key2, resource2)
        }
    }
}
