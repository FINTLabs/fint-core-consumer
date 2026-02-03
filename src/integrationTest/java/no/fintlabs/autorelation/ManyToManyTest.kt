package no.fintlabs.autorelation

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.entity.ConsumerRecordMetadata
import no.fintlabs.consumer.kafka.entity.KafkaEntity
import no.fintlabs.consumer.resource.ResourceService
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.elev.KontaktlarergruppeResource
import no.novari.fint.model.resource.utdanning.elev.UndervisningsforholdResource
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.time.Duration
import java.util.concurrent.TimeUnit

@SpringBootTest
@EmbeddedKafka(
    topics = ["fintlabs-no.fint-core.event.relation-update"],
    partitions = 1,
)
@ActiveProfiles("utdanning-elev")
class ManyToManyIntegrationTest(
    @Autowired private val cacheService: CacheService,
    @Autowired private val resourceService: ResourceService,
    @Autowired private val relationEventService: RelationEventService,
) {
    private val kontaktlarerId1 = "group-1"
    private val undervisningId1 = "und-auto-1"
    private val undervisningId2 = "und-auto-2"
    private val undervisningId3 = "und-auto-3"

    private val expectedBackLinkHref =
        "https://test.felleskomponent.no/utdanning/elev/kontaktlarergruppe/systemId/$kontaktlarerId1"
    private val backRelationName = "kontaktlarergruppe"

    @Test
    fun `Should automatically update existing Undervisningsforhold when a new Kontaktlarergruppe links to them`() {
        populateCacheWithUndervisningsforhold()

        val groupResource =
            createKontaktlarergruppe(kontaktlarerId1).apply {
                addUndervisningsforhold(Link.with("systemid/$undervisningId1"))
                addUndervisningsforhold(Link.with("systemid/$undervisningId2"))
                addUndervisningsforhold(Link.with("systemid/$undervisningId3"))
            }

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(kontaktlarerId1, "kontaktlarergruppe", groupResource),
        )
        relationEventService.addRelations("kontaktlarergruppe", kontaktlarerId1, groupResource)

        await().atMost(2, TimeUnit.SECONDS).untilAsserted {
            assertLinkExistsOnUndervisningsforhold(undervisningId1)
            assertLinkExistsOnUndervisningsforhold(undervisningId2)
            assertLinkExistsOnUndervisningsforhold(undervisningId3)
        }
    }

    @Test
    fun `Should remove relation from Undervisningsforhold when link is removed`() {
        populateCacheWithUndervisningsforhold()

        val initialGroupResource =
            createKontaktlarergruppe(kontaktlarerId1).apply {
                addUndervisningsforhold(Link.with("systemid/$undervisningId1"))
                addUndervisningsforhold(Link.with("systemid/$undervisningId2"))
                addUndervisningsforhold(Link.with("systemid/$undervisningId3"))
            }

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(kontaktlarerId1, "kontaktlarergruppe", initialGroupResource),
        )
        relationEventService.addRelations("kontaktlarergruppe", kontaktlarerId1, initialGroupResource)

        await().atMost(2, TimeUnit.SECONDS).untilAsserted {
            assertLinkExistsOnUndervisningsforhold(undervisningId1)
        }

        val updatedGroupResource =
            createKontaktlarergruppe(kontaktlarerId1).apply {
                addUndervisningsforhold(Link.with("systemid/$undervisningId1"))
                // Removed: addUndervisningsforhold(Link.with("systemid/$undervisningId2"))
                addUndervisningsforhold(Link.with("systemid/$undervisningId3"))
            }

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(kontaktlarerId1, "kontaktlarergruppe", updatedGroupResource),
        )

        await().atMost(2, TimeUnit.SECONDS).untilAsserted {
            assertLinkExistsOnUndervisningsforhold(undervisningId1)
            assertLinkExistsOnUndervisningsforhold(undervisningId3)

            val cachedGroup = cacheService.getCache("undervisningsforhold").get(undervisningId2)
            assertLinkWithHrefDoesNotExist(cachedGroup, backRelationName, expectedBackLinkHref)
        }
    }

    @Test
    fun `Should NOT update Kontaktlarergruppe when Undervisningsforhold adds a link (Inverse Side Check)`() {
        val groupResource = createKontaktlarergruppe(kontaktlarerId1)
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(kontaktlarerId1, "kontaktlarergruppe", groupResource),
        )

        val undervisningResource =
            createUndervisningsforholdResource(undervisningId1).apply {
                addLink(backRelationName, Link.with("systemid/$kontaktlarerId1"))
            }

        resourceService.processEntityConsumerRecord(createKafkaEntity(undervisningId1, "undervisningsforhold", undervisningResource))
        relationEventService.addRelations("undervisningsforhold", undervisningId1, undervisningResource)

        // We deliberately wait 500ms to allow potential bad events to process, then assert nothing changed.
        await()
            .pollDelay(Duration.ofMillis(500))
            .atMost(Duration.ofMillis(1000))
            .untilAsserted {
                val cachedGroup1 = cacheService.getCache("kontaktlarergruppe").get(kontaktlarerId1)
                assertNotNull(cachedGroup1)
                val links = cachedGroup1.links["undervisningsforhold"]
                assertTrue(
                    links.isNullOrEmpty(),
                    "Kontaktlarergruppe should not be updated by Undervisningsforhold (Slave)",
                )
            }
    }

    @Test
    fun `Should preserve existing Kontaktlarergruppe links when Undervisningsforhold updates`() {
        populateCacheWithUndervisningsforhold()
        val groupResource =
            createKontaktlarergruppe(kontaktlarerId1).apply {
                addUndervisningsforhold(Link.with("systemid/$undervisningId1"))
            }

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(kontaktlarerId1, "kontaktlarergruppe", groupResource),
        )
        relationEventService.addRelations("kontaktlarergruppe", kontaktlarerId1, groupResource)

        await().atMost(2, TimeUnit.SECONDS).untilAsserted {
            assertLinkExistsOnUndervisningsforhold(undervisningId1)
        }

        val freshUndervisningFromAdapter = createUndervisningsforholdResource(undervisningId1)

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId1, "undervisningsforhold", freshUndervisningFromAdapter),
        )

        await().atMost(2, TimeUnit.SECONDS).untilAsserted {
            val cachedUndervisning = cacheService.getCache("undervisningsforhold").get(undervisningId1)
            assertNotNull(cachedUndervisning)
            assertLinkWithHrefExists(cachedUndervisning, backRelationName, expectedBackLinkHref)
        }
    }

    private fun populateCacheWithUndervisningsforhold() {
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId1, "undervisningsforhold", createUndervisningsforholdResource(undervisningId1)),
        )
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId2, "undervisningsforhold", createUndervisningsforholdResource(undervisningId1)),
        )
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId3, "undervisningsforhold", createUndervisningsforholdResource(undervisningId1)),
        )
    }

    private fun assertLinkExistsOnUndervisningsforhold(undervisningId: String) {
        val undervisning = cacheService.getCache("undervisningsforhold").get(undervisningId)
        assertLinkWithHrefExists(undervisning, backRelationName, expectedBackLinkHref)
    }

    private fun assertLinkWithHrefExists(
        resource: FintResource?,
        relationName: String,
        expectedHref: String,
    ) {
        assertNotNull(resource, "Resource should be present in cache")
        val links = resource!!.links[relationName]
        assertNotNull(links, "Relation '$relationName' should exist in cached resource")

        val match = links!!.any { it.href.equals(expectedHref, ignoreCase = true) }

        assertTrue(
            match,
            "Expected link '$expectedHref' was not found in relation '$relationName'. Found: ${links.map { it.href }}",
        )
    }

    private fun assertLinkWithHrefDoesNotExist(
        resource: FintResource?,
        relationName: String,
        unexpectedHref: String,
    ) {
        assertNotNull(resource, "Resource should be present in cache")
        val links = resource!!.links[relationName]

        if (links.isNullOrEmpty()) return

        val match = links.any { it.href.equals(unexpectedHref, ignoreCase = true) }

        assertFalse(
            match,
            "Link '$unexpectedHref' should NOT be present in relation '$relationName', but it was found.",
        )
    }

    private fun createKontaktlarergruppe(id: String) =
        KontaktlarergruppeResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
            addSkole(Link.with("systemid/dummy-skole"))
        }

    private fun createUndervisningsforholdResource(id: String) =
        UndervisningsforholdResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }

    private fun createKafkaEntity(
        id: String,
        resourceName: String,
        resource: FintResource,
    ) = KafkaEntity(
        key = id,
        resourceName = resourceName,
        resource = resource,
        lastModified = System.currentTimeMillis(),
        retentionTime = null,
        consumerRecordMetadata = ConsumerRecordMetadata(SyncType.FULL, id, 1L),
    )
}
