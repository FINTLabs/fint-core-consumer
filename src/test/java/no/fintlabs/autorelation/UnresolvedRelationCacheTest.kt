package no.fintlabs.autorelation

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.mockk.mockk
import no.fintlabs.autorelation.buffer.RelationKey
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.novari.fint.model.resource.Link
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class UnresolvedRelationCacheTest {
    private lateinit var cache: Cache<RelationKey, MutableList<Link>>
    private lateinit var service: UnresolvedRelationCache

    private val resource = "person"
    private val resourceId = "123"
    private val relation = "manager"
    private val key = RelationKey(resource, resourceId, relation)

    @BeforeEach
    fun setup() {
        cache = Caffeine.newBuilder().build()
        service = UnresolvedRelationCache(cache)
    }

    @Test
    fun `registerLinks stores links under the correct key`() {
        val l1 = mockk<Link>(relaxed = true)

        service.registerRelation(resource, resourceId, relation, l1)

        val stored = cache.asMap()[key]
        Assertions.assertNotNull(stored, "Expected links to be stored after registerLinks")
        Assertions.assertEquals(listOf(l1), stored!!.toList(), "Stored links should match what was registered")
    }

    @Test
    fun `pollLinks returns all stored links and clears the entry`() {
        val l1 = mockk<Link>(relaxed = true)
        val l2 = mockk<Link>(relaxed = true)
        cache.put(key, mutableListOf(l1, l2))

        val polled = service.takeRelations(resource, resourceId, relation)
        Assertions.assertEquals(listOf(l1, l2), polled, "pollLinks should return all stored links")
        Assertions.assertFalse(cache.asMap().containsKey(key), "Entry should be removed from cache after polling")
    }

    @Test
    fun `registerLinks appends when called multiple times`() {
        val l1 = mockk<Link>(relaxed = true)
        val l2 = mockk<Link>(relaxed = true)

        service.registerRelation(resource, resourceId, relation, l1)
        service.registerRelation(resource, resourceId, relation, l2)

        val stored = cache.asMap()[key]!!.toList()
        Assertions.assertEquals(listOf(l1, l2), stored, "Links should accumulate across registrations")
    }
}