package no.fintlabs.consumer.links.relation

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class FintResourceExtensionsTest {
    private lateinit var resource: FintResource

    @BeforeEach
    fun setUp() {
        resource = ElevfravarResource()
    }

    @Test
    fun `applyUpdate ADD should add link if relation is missing`() {
        val link = Link.with("systemid/1")
        val update = createUpdate(RelationOperation.ADD, "rel-1", link)

        resource.applyUpdate(update)

        val links = resource.links["rel-1"]
        assertNotNull(links)
        assertEquals(1, links!!.size)
        assertEquals(link.href, links[0].href)
    }

    @Test
    fun `applyUpdate ADD should ignore duplicate link`() {
        val link = Link.with("systemid/1")
        resource.addUniqueLinks("rel-1", listOf(link))

        val update = createUpdate(RelationOperation.ADD, "rel-1", Link.with("systemid/1"))
        resource.applyUpdate(update)

        val links = resource.links["rel-1"]
        assertEquals(1, links!!.size)
    }

    @Test
    fun `applyUpdate DELETE should remove link`() {
        val link1 = Link.with("systemid/1")
        val link2 = Link.with("systemid/2")
        resource.addUniqueLinks("rel-1", listOf(link1, link2))

        val update = createUpdate(RelationOperation.DELETE, "rel-1", link1)
        resource.applyUpdate(update)

        val links = resource.links["rel-1"]
        assertNotNull(links)
        assertEquals(1, links!!.size)
        assertEquals(link2.href, links[0].href)
    }

    @Test
    fun `applyUpdate DELETE should remove relation key if list becomes empty`() {
        val link = Link.with("systemid/1")
        resource.addUniqueLinks("rel-1", listOf(link))

        val update = createUpdate(RelationOperation.DELETE, "rel-1", link)
        resource.applyUpdate(update)

        assertFalse(resource.links.containsKey("rel-1"))
    }

    @Test
    fun `applyUpdate DELETE should do nothing if relation does not exist`() {
        val link = Link.with("systemid/1")
        val update = createUpdate(RelationOperation.DELETE, "non-existent-rel", link)

        resource.applyUpdate(update)

        assertFalse(resource.links.containsKey("non-existent-rel"))
    }

    @Test
    fun `addUniqueLinks should add multiple unique links`() {
        val link1 = Link.with("systemid/1")
        val link2 = Link.with("systemid/2")

        resource.addUniqueLinks("rel-1", listOf(link1, link2))

        assertEquals(2, resource.links["rel-1"]!!.size)
    }

    @Test
    fun `addUniqueLinks should ignore empty list`() {
        resource.addUniqueLinks("rel-1", emptyList())
        assertFalse(resource.links.containsKey("rel-1"))
    }

    @Test
    fun `addUniqueLinks should handle case insensitive duplicates`() {
        val link1 = Link.with("systemid/abc")
        resource.addUniqueLinks("rel-1", listOf(link1))

        val link2 = Link.with("systemid/ABC")
        resource.addUniqueLinks("rel-1", listOf(link2))

        assertEquals(1, resource.links["rel-1"]!!.size)
    }

    @Test
    fun `removeRelationLink should remove partial match`() {
        val fullLink = Link.with("model/elev/123")
        resource.addUniqueLinks("elev", listOf(fullLink))

        val partialLink = Link.with("elev/123")
        resource.removeRelationLink("elev", partialLink)

        assertFalse(resource.links.containsKey("elev"))
    }

    @Test
    fun `MutableList addUniqueLink should return false if exists`() {
        val list = mutableListOf(Link.with("systemid/1"))
        val wasAdded = list.addUniqueLink(Link.with("systemid/1"))

        assertFalse(wasAdded)
        assertEquals(1, list.size)
    }

    @Test
    fun `MutableList removeMatchingLink should return true if removed`() {
        val list = mutableListOf(Link.with("systemid/1"))
        val wasRemoved = list.removeMatchingLink(Link.with("systemid/1"))

        assertTrue(wasRemoved)
        assertTrue(list.isEmpty())
    }

    private fun createUpdate(
        operation: RelationOperation,
        rel: String,
        link: Link,
    ): RelationUpdate =
        RelationUpdate(
            orgId = "fintlabs.no",
            targetEntity = EntityDescriptor("utdanning", "vurdering", "elevfravar"),
            targetId = "123",
            binding = RelationBinding(rel, link),
            operation = operation,
        )
}
