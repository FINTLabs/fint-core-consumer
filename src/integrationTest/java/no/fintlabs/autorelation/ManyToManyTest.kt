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
import org.junit.jupiter.api.Disabled
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

    // The link that gets added/removed on the parent resource
    private val expectedBackLinkHref =
        "https://test.felleskomponent.no/utdanning/elev/undervisningsforhold/systemId/$undervisningId"
    private val backRelationName = "undervisningsforhold"

    @Test
    fun `Should automatically update existing Kontaktlarergrupper when a new Undervisningsforhold links to them`() {
        populateCacheWithGroups()

        val undervisningResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                addKontaktlarergruppe(Link.with("systemid/$groupId2"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
                addMandatoryLinks()
            }

        // Simulating the flow: Resource enters cache -> Event Service adds relations
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
    @Disabled // TODO: Bug, doesn't remove 'removed' links from resource & sends wrong relation update
    fun `Should remove relation from Kontaktlarergruppe when link is removed`() {
        populateCacheWithGroups()

        // 1. STATE 1: Resource has links to G1, G2, G3
        val initialResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                addKontaktlarergruppe(Link.with("systemid/$groupId2"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
                addMandatoryLinks()
            }

        // Put initial state in Cache & Relations
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId, "undervisningsforhold", initialResource),
        )
        relationEventService.addRelations("undervisningsforhold", undervisningId, initialResource)
        Thread.sleep(1000)

        // Verify initial state
        assertLinkExistsOnGroup(groupId2)

        // 2. STATE 2: Update Resource - REMOVE G2 (Only G1 and G3 remain)
        val updatedResource =
            createUndervisningsforholdResource(undervisningId).apply {
                addKontaktlarergruppe(Link.with("systemid/$groupId1"))
                // Removed: addKontaktlarergruppe(Link.with("systemid/$groupId2"))
                addKontaktlarergruppe(Link.with("systemid/$groupId3"))
                addMandatoryLinks()
            }

        // 3. TRIGGER UPDATE:
        // Calling processEntityConsumerRecord with the new resource does two things:
        // A. It triggers autoRelationService.reconcileLinks() -> which finds the diff and removes old relations (G1, G2, G3)
        // B. It updates the local cache with the new resource.
        resourceService.processEntityConsumerRecord(
            createKafkaEntity(undervisningId, "undervisningsforhold", updatedResource),
        )

        // 4. TRIGGER ADD:
        // Since the previous step (Pruning) effectively wiped the relations based on the old resource,
        // we must now apply the "Add" for the new resource to restore G1 and G3.
        relationEventService.addRelations("undervisningsforhold", undervisningId, updatedResource)

        Thread.sleep(1000)

        // 5. VERIFY
        // Group 1 & 3 should be present (restored)
        assertLinkExistsOnGroup(groupId1)
        assertLinkExistsOnGroup(groupId3)

        // Group 2 should be gone (pruned and not restored)
        val cachedGroup2 = cacheService.getCache("kontaktlarergruppe").get(groupId2)
        assertLinkWithHrefDoesNotExist(cachedGroup2, backRelationName, expectedBackLinkHref)
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

    private fun UndervisningsforholdResource.addMandatoryLinks() {
        addSkole(Link.with("systemid/dummy-skole"))
        addSkoleressurs(Link.with("systemid/dummy-skoleressurs"))
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
