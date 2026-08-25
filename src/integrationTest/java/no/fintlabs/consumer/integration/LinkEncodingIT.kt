package no.fintlabs.consumer.integration

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.Application
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.utils.EntityProducer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.utdanning.timeplan.FagResource
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.test.web.client.getForEntity
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.HttpStatus
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import java.net.URI
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = [Application::class])
@EmbeddedKafka(partitions = 1)
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=link-encoding-it",
        "novari.kafka.default-replicas=1",
        "fint.relation.base-url=https://foo.org",
        "fint.org-id=foo.org",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package=timeplan",
        "fint.security.enabled=false",
    ],
)
@DirtiesContext
class LinkEncodingIT {
    @Autowired
    lateinit var rest: TestRestTemplate

    @Autowired
    lateinit var objectMapper: ObjectMapper

    @Autowired
    lateinit var cacheService: CacheService

    @Autowired
    lateinit var entityProducer: EntityProducer

    @LocalServerPort
    private var port: Int = 0

    @AfterEach
    fun tearDown() {
        cacheService.getCache("fag").evictExpired(Long.MAX_VALUE)
    }

    @Test
    fun `idValue is percent-encoded in responses, stays decoded in the cache, and the encoded link resolves`() {
        val id = "fag A#1"
        publishFag(id)

        var selfHref = ""
        await.atMost(Duration.ofSeconds(10)).untilAsserted {
            val page = objectMapper.readTree(fetchAllFagJson())
            val entries = page["_embedded"]["_entries"]
            assertEquals(1, entries.size(), "The cache should contain one entry")
            selfHref = entries[0]["_links"]["self"][0]["href"].asText()
        }

        assertTrue(
            selfHref.endsWith("/utdanning/timeplan/fag/systemid/fag%20A%231"),
            "Self href should be percent-encoded, got: $selfHref",
        )

        val cachedResource = cacheService.getCache("fag").get(id)
        assertNotNull(cachedResource, "Resource should be cached under its decoded id")
        val cachedHref = cachedResource.selfLinks.first().href
        assertTrue(
            cachedHref.endsWith("/utdanning/timeplan/fag/systemid/fag A#1"),
            "Cached href should stay decoded, got: $cachedHref",
        )

        val response = rest.getForEntity(toLocalUri(selfHref), String::class.java)
        assertEquals(HttpStatus.OK, response.statusCode, "Encoded self link should resolve to the resource")
        val body = objectMapper.readTree(assertNotNull(response.body))
        assertEquals(id, body["systemId"]["identifikatorverdi"].asText())
    }

    private fun publishFag(id: String) {
        val fag =
            FagResource().apply {
                systemId = Identifikator().apply { identifikatorverdi = id }
                navn = "Fag $id"
            }
        entityProducer
            .publish("fag", fag, id, SyncType.FULL, UUID.randomUUID().toString(), 1)
            .get(10, TimeUnit.SECONDS)
    }

    private fun fetchAllFagJson(): String {
        val response = rest.getForEntity<String>("/utdanning/timeplan/fag")
        assertEquals(HttpStatus.OK, response.statusCode)
        return assertNotNull(response.body)
    }

    private fun toLocalUri(href: String): URI = URI.create("http://localhost:$port" + href.replace(Regex("^https?://[^:/]+(:\\d+)?"), ""))
}
