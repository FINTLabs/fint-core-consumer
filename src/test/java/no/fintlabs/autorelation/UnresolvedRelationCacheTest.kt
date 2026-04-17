package no.fintlabs.autorelation

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.consumer.config.AutorelationConfig
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.fint.model.resource.Link
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.Duration

class UnresolvedRelationCacheTest {
    private lateinit var service: UnresolvedRelationCache
    private lateinit var meterRegistry: MeterRegistry

    private val resource = "person"
    private val resourceId = "123"
    private val relation = "manager"

    private fun cacheWithTtl(
        ttl: Duration,
        registry: MeterRegistry = SimpleMeterRegistry(),
    ): UnresolvedRelationCache {
        val config = mockk<ConsumerConfiguration>()
        every { config.autorelation } returns AutorelationConfig(buffer = AutorelationConfig.BufferConfig(ttl = ttl))
        return UnresolvedRelationCache(config, registry)
    }

    @BeforeEach
    fun setup() {
        meterRegistry = SimpleMeterRegistry()
        service = cacheWithTtl(Duration.ofDays(7), meterRegistry)
    }

    private fun bufferCounter(outcome: String): Double =
        meterRegistry
            .find("fint.autorelation.buffer.records")
            .tag("resource", resource)
            .tag("relation", relation)
            .tag("outcome", outcome)
            .counter()
            ?.count() ?: 0.0

    @Test
    fun `registerRelation stores link and takeRelations retrieves it`() {
        val link = Link.with("http://test-link")

        service.registerRelation(resource, resourceId, relation, link, System.currentTimeMillis())

        val result = service.takeRelations(resource, resourceId, relation)
        assertEquals(listOf(link), result)
    }

    @Test
    fun `takeRelations returns empty list when no links exist`() {
        val result = service.takeRelations(resource, resourceId, relation)
        assertEquals(emptyList<Link>(), result)
    }

    @Test
    fun `takeRelations clears the entry after retrieval`() {
        val link = Link.with("http://test-link")
        service.registerRelation(resource, resourceId, relation, link, System.currentTimeMillis())

        service.takeRelations(resource, resourceId, relation)
        val secondCall = service.takeRelations(resource, resourceId, relation)

        assertEquals(emptyList<Link>(), secondCall)
    }

    @Test
    fun `registered counter increments on first registerRelation for a key`() {
        service.registerRelation(resource, resourceId, relation, Link.with("http://l"), System.currentTimeMillis())

        assertEquals(1.0, bufferCounter("registered"))
        assertEquals(0.0, bufferCounter("appended"))
    }

    @Test
    fun `drained counter increments by the number of links returned`() {
        val now = System.currentTimeMillis()
        service.registerRelation(resource, resourceId, relation, Link.with("http://l1"), now)
        service.registerRelation(resource, resourceId, relation, Link.with("http://l2"), now)
        service.registerRelation(resource, resourceId, relation, Link.with("http://l3"), now)

        val drained = service.takeRelations(resource, resourceId, relation)

        assertEquals(3, drained.size)
        assertEquals(3.0, bufferCounter("drained"))
    }

    @Test
    fun `buffer size gauge reflects the number of entries in the cache`() {
        service.registerRelation(resource, resourceId, relation, Link.with("http://l1"), System.currentTimeMillis())
        service.registerRelation(resource, "other-id", relation, Link.with("http://l2"), System.currentTimeMillis())

        val size =
            meterRegistry
                .find("fint.autorelation.buffer.size")
                .gauge()
                ?.value() ?: 0.0
        assertEquals(2.0, size)
    }

    @Test
    fun `expired counter fires when a live buffer entry expires via TTL`() {
        val registry = SimpleMeterRegistry()
        val shortCache = cacheWithTtl(Duration.ofMillis(500), registry)

        shortCache.registerRelation(resource, resourceId, relation, Link.with("http://l1"), System.currentTimeMillis())
        shortCache.registerRelation(resource, resourceId, relation, Link.with("http://l2"), System.currentTimeMillis())

        // Variable-expiry Caffeine uses a hierarchical wheel; give it ample time past the TTL
        // and call cleanUp() to force maintenance-time eviction.
        org.awaitility.kotlin.await
            .atMost(java.time.Duration.ofSeconds(5))
            .until {
                shortCache.cleanUp()
                val count =
                    registry
                        .find("fint.autorelation.buffer.records")
                        .tag("resource", resource)
                        .tag("relation", relation)
                        .tag("outcome", "expired")
                        .counter()
                        ?.count() ?: 0.0
                count >= 2.0
            }
    }

    @Test
    fun `stillborn counter fires when createdAt is older than TTL`() {
        val shortCache = cacheWithTtl(Duration.ofSeconds(1), meterRegistry)
        val staleTimestamp = System.currentTimeMillis() - Duration.ofSeconds(30).toMillis()

        shortCache.registerRelation(resource, resourceId, relation, Link.with("http://stale"), staleTimestamp)

        assertEquals(1.0, bufferCounter("stillborn"))
        assertEquals(0.0, bufferCounter("registered"), "stillborn entry should not count as registered")
        assertEquals(emptyList<Link>(), shortCache.takeRelations(resource, resourceId, relation))
    }

    @Test
    fun `removed_by_delete counter increments when a buffered link is removed`() {
        val now = System.currentTimeMillis()
        val link1 = Link.with("http://l1")
        val link2 = Link.with("http://l2")
        service.registerRelation(resource, resourceId, relation, link1, now)
        service.registerRelation(resource, resourceId, relation, link2, now)

        service.removeRelation(resource, resourceId, relation, link1)

        assertEquals(1.0, bufferCounter("removed_by_delete"))
    }

    @Test
    fun `removed_by_delete counter does not fire when the target link is not present`() {
        service.removeRelation(resource, resourceId, relation, Link.with("http://not-there"))

        assertEquals(0.0, bufferCounter("removed_by_delete"))
    }

    @Test
    fun `drained counter does not increment when takeRelations finds nothing`() {
        service.takeRelations(resource, resourceId, relation)

        assertEquals(0.0, bufferCounter("drained"))
    }

    @Test
    fun `appended counter increments when registerRelation adds to an existing key`() {
        val now = System.currentTimeMillis()
        service.registerRelation(resource, resourceId, relation, Link.with("http://l1"), now)
        service.registerRelation(resource, resourceId, relation, Link.with("http://l2"), now)
        service.registerRelation(resource, resourceId, relation, Link.with("http://l3"), now)

        assertEquals(1.0, bufferCounter("registered"))
        assertEquals(2.0, bufferCounter("appended"))
    }

    @Test
    fun `registerRelation appends when called multiple times`() {
        val l1 = Link.with("http://link-1")
        val l2 = Link.with("http://link-2")
        val now = System.currentTimeMillis()

        service.registerRelation(resource, resourceId, relation, l1, now)
        service.registerRelation(resource, resourceId, relation, l2, now)

        val result = service.takeRelations(resource, resourceId, relation)
        assertEquals(listOf(l1, l2), result)
    }

    @Test
    fun `removeRelation removes specific link`() {
        val l1 = Link.with("http://link-1")
        val l2 = Link.with("http://link-2")
        val now = System.currentTimeMillis()

        service.registerRelation(resource, resourceId, relation, l1, now)
        service.registerRelation(resource, resourceId, relation, l2, now)

        service.removeRelation(resource, resourceId, relation, l1)

        val result = service.takeRelations(resource, resourceId, relation)
        assertEquals(listOf(l2), result)
    }

    @Test
    fun `removeRelation cleans up entry when last link is removed`() {
        val link = Link.with("http://link-1")
        service.registerRelation(resource, resourceId, relation, link, System.currentTimeMillis())

        service.removeRelation(resource, resourceId, relation, link)

        val result = service.takeRelations(resource, resourceId, relation)
        assertEquals(emptyList<Link>(), result)
    }

    @Nested
    inner class DynamicRetentionScenarios {
        @Test
        fun `entry with expired timestamp is evicted`() {
            val eightDaysAgo = System.currentTimeMillis() - Duration.ofDays(8).toMillis()
            val link = Link.with("http://expired-link")

            service.registerRelation(resource, resourceId, relation, link, eightDaysAgo)
            service.cleanUp()

            assertEquals(emptyList<Link>(), service.takeRelations(resource, resourceId, relation))
        }

        @Test
        fun `entry with recent timestamp is retained`() {
            val oneDayAgo = System.currentTimeMillis() - Duration.ofDays(1).toMillis()
            val link = Link.with("http://fresh-link")

            service.registerRelation(resource, resourceId, relation, link, oneDayAgo)
            service.cleanUp()

            assertEquals(listOf(link), service.takeRelations(resource, resourceId, relation))
        }

        @Test
        fun `entry exactly at TTL boundary is evicted`() {
            val exactlySevenDaysAgo = System.currentTimeMillis() - Duration.ofDays(7).toMillis()
            val link = Link.with("http://boundary-link")

            service.registerRelation(resource, resourceId, relation, link, exactlySevenDaysAgo)
            service.cleanUp()

            assertEquals(emptyList<Link>(), service.takeRelations(resource, resourceId, relation))
        }
    }

    @Nested
    inner class ConfigurableTtl {
        @Test
        fun `custom short TTL evicts entries older than that duration`() {
            val cache = cacheWithTtl(Duration.ofMinutes(5))
            val link = Link.with("http://expired-link")
            val tenMinutesAgo = System.currentTimeMillis() - Duration.ofMinutes(10).toMillis()

            cache.registerRelation(resource, resourceId, relation, link, tenMinutesAgo)
            cache.cleanUp()

            assertEquals(emptyList<Link>(), cache.takeRelations(resource, resourceId, relation))
        }

        @Test
        fun `custom short TTL retains entries younger than that duration`() {
            val cache = cacheWithTtl(Duration.ofMinutes(10))
            val link = Link.with("http://fresh-link")
            val fiveMinutesAgo = System.currentTimeMillis() - Duration.ofMinutes(5).toMillis()

            cache.registerRelation(resource, resourceId, relation, link, fiveMinutesAgo)
            cache.cleanUp()

            assertEquals(listOf(link), cache.takeRelations(resource, resourceId, relation))
        }

        @Test
        fun `default TTL is 30 days`() {
            assertEquals(Duration.ofDays(30), AutorelationConfig.BufferConfig().ttl)
        }
    }
}
