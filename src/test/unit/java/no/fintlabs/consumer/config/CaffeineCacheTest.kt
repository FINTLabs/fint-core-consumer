package no.fintlabs.consumer.config

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.RemovalCause
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.consumer.kafka.sync.SyncStatusProducer
import no.fintlabs.consumer.kafka.sync.model.SyncKey
import no.fintlabs.consumer.kafka.sync.model.SyncState
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import java.time.Duration
import java.util.*

class CaffeineCacheTest {
    private lateinit var producer: SyncStatusProducer
    private lateinit var properties: CaffeineCacheProperties
    private lateinit var cacheConfig: CaffeineCacheConfig
    private lateinit var syncKey: SyncKey
    private lateinit var syncState: SyncState
    private lateinit var cause: RemovalCause

    private lateinit var sut: Cache<SyncKey, SyncState>

    @BeforeEach
    fun setUp() {
        producer = mockk(relaxed = true)
        properties = mockk(relaxed = true)
        syncKey =
            mockk(relaxed = true) {
                every { type } returns SyncType.FULL
            }
        syncState =
            mockk(relaxed = true) {
                every { corrId } returns UUID.randomUUID().toString()
            }
        cause = RemovalCause.EXPLICIT

        cacheConfig = CaffeineCacheConfig(producer, properties)
    }

    @Test
    fun `syncStateCache expires after correct configuration`() {
        every { properties.expireAfterAccess } returns Duration.ofMillis(50)
        val cache = cacheConfig.syncStateCache()

        cache.put(syncKey, syncState)
        assertNotNull(cache.getIfPresent(syncKey))
        Thread.sleep(60)
        assertNull(cache.getIfPresent(syncKey))
    }

    @Test
    fun `syncStateCache publishes one SyncStatus on removal`() {
        every { properties.expireAfterAccess } returns Duration.ofMillis(50)
        val cache = cacheConfig.syncStateCache()

        cache.put(syncKey, syncState)
        Thread.sleep(60)
        assertNull(cache.getIfPresent(syncKey))
        verify(exactly = 1) { producer.publish(any()) }
    }
}
