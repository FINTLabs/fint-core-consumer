package no.fintlabs.consumer.integration

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.config.KafkaTestConfig
import no.fintlabs.consumer.admin.CacheEntry
import no.fintlabs.consumer.resource.dto.LastUpdatedResponse
import no.fintlabs.utils.EntityProducer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.context.annotation.Import
import org.springframework.core.ParameterizedTypeReference
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@EmbeddedKafka(
    partitions = 1,
    topics = ["fintlabs-no.fint-core.entity.utdanning-vurdering"],
)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "fint.security.enabled=false",
    ],
)
@ActiveProfiles("utdanning-vurdering")
@Import(KafkaTestConfig::class)
class SyncTimestampIT {
    @LocalServerPort
    private var port: Int = 0

    private val client by lazy {
        WebTestClient.bindToServer().baseUrl("http://localhost:$port").build()
    }

    private val resourceName = "elevfravar"

    @Autowired
    lateinit var entityProducer: EntityProducer

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var cacheService: CacheService

    @AfterEach
    fun tearDown() {
        cacheService.getCache(resourceName).evictExpired(Long.MAX_VALUE)
    }

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.BEFORE_METHOD)
    fun `before any full sync, lastFullSync is epoch in last-updated response`() {
        val lastUpdated = getLastUpdated()
        assertEquals(
            0L,
            lastUpdated.lastCompletedFullSync.time,
            "lastCompletedFullSync should be epoch before any full sync",
        )
    }

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.BEFORE_METHOD)
    fun `before any full sync, admin cache status shows epoch for lastFullSync`() {
        val cacheStatus = getCacheStatus()
        val fagEntry = cacheStatus[resourceName] ?: error("No fag entry in cache status")
        assertEquals(
            0L,
            fagEntry.lastCompletedFullSync.time,
            "lastCompletedFullSync should be epoch before any full sync",
        )
    }

    @Test
    fun `after full sync completes, last-updated response contains sync timestamp`() {
        val syncTimestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS).toEpochMilli()
        sendFullSync(timestamp = syncTimestamp)

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val lastUpdated = getLastUpdated()
            assertTrue(
                lastUpdated.lastCompletedFullSync.time > 0,
                "lastCompletedFullSync should be non-epoch after full sync",
            )
            assertEquals(
                syncTimestamp,
                lastUpdated.lastCompletedFullSync.time,
                "lastCompletedFullSync should match the timestamp of the completed sync",
            )
        }
    }

    @Test
    fun `after full sync completes, admin cache status contains sync timestamp`() {
        val syncTimestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS).toEpochMilli()
        sendFullSync(timestamp = syncTimestamp)

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val fagEntry = getCacheStatus()[resourceName] ?: error("No fag entry in cache status")
            assertTrue(
                fagEntry.lastCompletedFullSync.time > 0,
                "lastCompletedFullSync should be non-epoch after full sync",
            )
            assertEquals(
                syncTimestamp,
                fagEntry.lastCompletedFullSync.time,
                "lastCompletedFullSync in admin cache status should match the completed sync timestamp",
            )
        }
    }

    @Test
    fun `second full sync updates lastFullSync to the newer timestamp`() {
        val firstTimestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS).toEpochMilli()
        sendFullSync(timestamp = firstTimestamp)

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertEquals(firstTimestamp, getLastUpdated().lastCompletedFullSync.time)
        }

        val corrId = UUID.randomUUID().toString()
        val secondTimestamp = firstTimestamp + 5_000
        sendFullSync(timestamp = secondTimestamp, syncCorrId = corrId, syncTotalSize = 2)
        sendFullSync(timestamp = secondTimestamp, syncCorrId = corrId, syncTotalSize = 2)

        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            assertEquals(
                secondTimestamp,
                getLastUpdated().lastCompletedFullSync.time,
                "lastCompletedFullSync should be updated to the second sync's timestamp",
            )
        }
    }

    private fun getLastUpdated(): LastUpdatedResponse =
        client
            .get()
            .uri("/utdanning/vurdering/elevfravar/last-updated")
            .exchange()
            .expectStatus()
            .isOk
            .expectBody<LastUpdatedResponse>()
            .returnResult()
            .responseBody!!

    private fun getCacheStatus(): Map<String, CacheEntry> =
        client
            .get()
            .uri("/utdanning/vurdering/admin/cache/status")
            .exchange()
            .expectStatus()
            .isOk
            .expectBody(object : ParameterizedTypeReference<Map<String, CacheEntry>>() {})
            .returnResult()
            .responseBody!!

    private fun sendFullSync(
        timestamp: Long = Instant.now().toEpochMilli(),
        syncTotalSize: Long = 1,
        syncCorrId: String = UUID.randomUUID().toString(),
    ) {
        val resourceId = UUID.randomUUID().toString()
        val resource =
            ElevfravarResource().apply {
                systemId =
                    Identifikator().apply {
                        identifikatorverdi = resourceId
                    }
            }

        entityProducer
            .publish(
                resourceName,
                resource,
                resourceId,
                SyncType.FULL,
                syncCorrId,
                syncTotalSize,
                timestamp,
            ).get(10, TimeUnit.SECONDS)
    }
}
