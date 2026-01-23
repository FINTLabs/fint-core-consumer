package no.fintlabs.cache

import io.mockk.*
import no.fintlabs.autorelation.RelationEventService
import no.fintlabs.cache.cacheObjects.CacheObject
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.function.BiConsumer

class CacheEvictionServiceTest {
    private val cacheService: CacheService = mockk(relaxed = true)
    private val relationEventService: RelationEventService = mockk(relaxed = true)

    private val service =
        CacheEvictionService(
            cacheService = cacheService,
            relationEventService = relationEventService,
        )

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Test
    fun `eviction does not trigger upon unknown resource`() {
        val resourceName = "unknown-resource"
        every { cacheService.getCache(resourceName) } returns null

        service.evictExpired(resourceName)

        verify { relationEventService wasNot Called }
    }

    @Test
    fun `calls removeRelations for every evicted object`() {
        val resourceName = "elevfravar"
        val key1 = "k1"
        val key2 = "k2"

        val fintCache = mockk<Cache<FintResource>>(relaxed = true)
        val resource1 = ElevfravarResource()
        val resource2 = ElevfravarResource()

        val c1 = mockk<CacheObject<FintResource>> { every { unboxObject() } returns resource1 }
        val c2 = mockk<CacheObject<FintResource>> { every { unboxObject() } returns resource2 }

        every { cacheService.getCache(resourceName) } returns fintCache

        every { fintCache.evictOldCacheObjects(any()) } answers {
            val callback = firstArg<BiConsumer<String, CacheObject<FintResource>>>()
            callback.accept(key1, c1)
            callback.accept(key2, c2)
        }

        service.evictExpired(resourceName)

        verify(exactly = 1) {
            relationEventService.removeRelations(resourceName, key1, resource1)
            relationEventService.removeRelations(resourceName, key2, resource2)
        }
    }
}
