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
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles

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
    private val groupId1 = "group-1"
    private val groupId2 = "group-2"
    private val groupId3 = "group-3"
    private val undervisningId = "und-auto-1"

    private val expectedBackLinkHref =
        "https://test.felleskomponent.no/utdanning/elev/undervisningsforhold/systemId/$undervisningId"
    private val backRelationName = "undervisningsforhold"

    // TODO: Remove Thread.sleep

    @Test
    fun `Should automatically update existing Kontaktlarergrupper when a new Undervisningsforhold links to them`() {
        populateCacheWithGroups()

        val undervisningResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                addKontaktlarergruppe(Link.with("systemid/$groupId2"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
            }

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId, "undervisningsforhold", undervisningResource),
        )
        relationEventService.addRelations("undervisningsforhold", undervisningId, undervisningResource)

        Thread.sleep(1000)

        assertLinkExistsOnGroup(groupId1)
        assertLinkExistsOnGroup(groupId2)
        assertLinkExistsOnGroup(groupId3)
    }

    @Test
    fun `Should remove relation from Kontaktlarergruppe when link is removed`() {
        populateCacheWithGroups()

        val initialResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                addKontaktlarergruppe(Link.with("systemid/$groupId2"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
            }

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId, "undervisningsforhold", initialResource),
        )
        relationEventService.addRelations("undervisningsforhold", undervisningId, initialResource)
        Thread.sleep(1000)

        assertLinkExistsOnGroup(groupId2)

        val updatedResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                // Removed: addKontaktlarergruppe(Link.with("systemid/$groupId2"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
            }

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId, "undervisningsforhold", updatedResource),
        )

        Thread.sleep(1000)

        assertLinkExistsOnGroup(groupId1)
        assertLinkExistsOnGroup(groupId3)

        // Group 2 should be gone (pruned and not restored)
        val cachedGroup2 = cacheService.getCache("kontaktlarergruppe").get(groupId2)
        assertLinkWithHrefDoesNotExist(cachedGroup2, backRelationName, expectedBackLinkHref)
    }

    @Test
    fun `Should NOT update Undervisningsforhold when Kontaktlarergruppe adds a link (Inverse Side Check)`() {
        val uResource = createUndervisningsforholdResource(undervisningId)
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(
                undervisningId,
                "undervisningsforhold",
                uResource,
            ),
        )

        val groupResource =
            createKontaktlarergruppe(groupId1).apply {
                addLink(backRelationName, Link.with("systemid/$undervisningId"))
            }

        resourceService.processEntityConsumerRecord(createKafkaEntity(groupId1, "kontaktlarergruppe", groupResource))

        relationEventService.addRelations("kontaktlarergruppe", groupId1, groupResource)

        Thread.sleep(1000)

        val cachedU1 = cacheService.getCache("undervisningsforhold").get(undervisningId)
        assertNotNull(cachedU1)

        val links = cachedU1.links["kontaktlarergruppe"]
        assertTrue(links.isNullOrEmpty(), "Undervisningsforhold should not be updated by Kontaktlarergruppe (Slave)")
    }

    @Test
    fun `Should preserve existing Undervisningsforhold links when Kontaktlarergruppe updates`() {
        populateCacheWithGroups()
        val uResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
            }

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(
                undervisningId,
                "undervisningsforhold",
                uResource,
            ),
        )
        relationEventService.addRelations("undervisningsforhold", undervisningId, uResource)
        Thread.sleep(1000)

        assertLinkExistsOnGroup(groupId1)

        val freshGroupFromAdapter = createKontaktlarergruppe(groupId1) // Contains no 'undervisningsforhold' links

        resourceService.processEntityConsumerRecord(
            createKafkaEntity(
                groupId1,
                "kontaktlarergruppe",
                freshGroupFromAdapter,
            ),
        )

        val cachedGroup = cacheService.getCache("kontaktlarergruppe").get(groupId1)
        assertNotNull(cachedGroup)

        assertLinkWithHrefExists(cachedGroup, backRelationName, expectedBackLinkHref)
    }

    private fun populateCacheWithGroups() {
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(groupId1, "kontaktlarergruppe", createKontaktlarergruppe(groupId1)),
        )
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(groupId2, "kontaktlarergruppe", createKontaktlarergruppe(groupId2)),
        )
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(groupId3, "kontaktlarergruppe", createKontaktlarergruppe(groupId3)),
        )
    }

    private fun assertLinkExistsOnGroup(groupId: String) {
        val group = cacheService.getCache("kontaktlarergruppe").get(groupId)
        assertLinkWithHrefExists(group, backRelationName, expectedBackLinkHref)
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

        // If null or empty, the link definitely doesn't exist
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
