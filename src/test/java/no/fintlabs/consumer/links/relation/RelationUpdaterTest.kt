//package no.fintlabs.consumer.links.relation
//
//import no.novari.fint.model.resource.Link
//import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
//import no.fintlabs.autorelation.model.RelationOperation
//import no.fintlabs.autorelation.model.RelationRef
//import no.fintlabs.autorelation.model.RelationUpdate
//import org.junit.jupiter.api.Assertions.*
//import org.junit.jupiter.api.Test
//import org.junit.jupiter.api.assertAll
//
//class RelationUpdaterTest {
//
//    private val updater = RelationUpdater()
//
//    private companion object {
//        const val REL_ELEVFORHOLD = "elevforhold"
//        const val REL_FRAVARSREGISTRERING = "fravarsregistrering"
//    }
//
//    @Test
//    fun `addLinks(list, toAdd) deduplicates by endsWith ignoreCase`() {
//        val target = mutableListOf(
//            link("https://test.felleskomponent.no/api/$REL_ELEVFORHOLD/ABC")
//        )
//        val toAdd = listOf(link("ABC"), link("aBc"))
//
//        updater.addLinks(target, toAdd)
//
//        assertAll(
//            { assertEquals(1, target.size, "No duplicates should be added when endsWith matches (ignoreCase)") },
//            { assertTrue(target.first().href.endsWith("ABC", ignoreCase = true)) }
//        )
//    }
//
//    @Test
//    fun `deleteLinks removes matches for ALL ids in RelationRef (not just first)`() {
//        val links = mutableListOf(
//            link("https://host/api/$REL_ELEVFORHOLD/1"),
//            link("https://host/api/$REL_ELEVFORHOLD/2"),
//            link("https://host/api/$REL_ELEVFORHOLD/3")
//        )
//        val ref = RelationRef(
//            name = REL_ELEVFORHOLD,
//            links = listOf(link("2"), link("3"))
//        )
//
//        // EXPECTED: both /2 and /3 removed; CURRENT impl leaves /3
//        updater.deleteLinks(links, ref)
//
//        val hrefs = links.hrefs()
//        assertAll(
//            { assertEquals(1, links.size, "Should remove both /2 and /3") },
//            { assertTrue(hrefs.any { it.endsWith("/1") }) },
//            { assertFalse(hrefs.any { it.endsWith("/2") }) },
//            { assertFalse(hrefs.any { it.endsWith("/3") }) }
//        )
//    }
//
//    @Test
//    fun `deleteLinks is no-op when no ids match (should not throw)`() {
//        val links = mutableListOf(
//            link("https://host/api/$REL_ELEVFORHOLD/10"),
//            link("https://host/api/$REL_ELEVFORHOLD/20")
//        )
//        val ref = RelationRef(
//            name = REL_ELEVFORHOLD,
//            links = listOf(link("999"), link("888"))
//        )
//
//        assertDoesNotThrow { updater.deleteLinks(links, ref) }
//        assertEquals(2, links.size)
//    }
//
//    @Test
//    fun `addLinks(list, toAdd) ignores duplicates within toAdd itself`() {
//        val target = mutableListOf<Link>()
//        val toAdd = listOf(link("ABC"), link("aBc"), link("AbC"))
//        updater.addLinks(target, toAdd)
//        assertAll(
//            { assertEquals(1, target.size) },
//            { assertTrue(target.first().href.endsWith("ABC", ignoreCase = true)) }
//        )
//    }
//
//    @Test
//    fun `addLinks(list, toAdd) treats query and fragment as part of id (no accidental dedupe)`() {
//        val target = mutableListOf(link("https://h/api/$REL_ELEVFORHOLD/123"))
//        val toAdd = listOf(
//            link("$REL_ELEVFORHOLD/123?x=1"),
//            link("$REL_ELEVFORHOLD/123#frag")
//        )
//        updater.addLinks(target, toAdd)
//        // Since endsWith compares the whole id string, these should be considered distinct and thus added
//        assertEquals(3, target.size)
//    }
//
//    @Test
//    fun `attachBuffered merges with existing and adds only missing`() {
//        val res = ElevfravarResource().apply {
//            linksFor(REL_FRAVARSREGISTRERING).add(link("$REL_FRAVARSREGISTRERING/42"))
//        }
//        val newLinks = listOf(
//            link("$REL_FRAVARSREGISTRERING/42"),
//            link("$REL_FRAVARSREGISTRERING/43")
//        )
//        updater.attachBuffered(res, REL_FRAVARSREGISTRERING, newLinks)
//
//        val stored = res.links[REL_FRAVARSREGISTRERING].orEmpty()
//        val hrefs = stored.hrefs()
//        assertAll(
//            { assertEquals(2, stored.size) },
//            { assertTrue(hrefs.any { it.endsWith("/42") }) },
//            { assertTrue(hrefs.any { it.endsWith("/43") }) }
//        )
//    }
//
//    @Test
//    fun `update with ADD creates relation list when absent and adds`() {
//        val res = ElevfravarResource() // assume links map is initialized
//        val update = RelationUpdate(
//            orgId = "org",
//            domainName = "dom",
//            packageName = "pkg",
//            resource = no.fintlabs.autorelation.model.ResourceRef("Elevforhold", "1"),
//            relation = RelationRef(REL_ELEVFORHOLD, listOf(link("$REL_ELEVFORHOLD/77"))),
//            operation = RelationOperation.ADD
//        )
//
//        updater.update(update, res)
//
//        val stored = res.linksFor(REL_ELEVFORHOLD)
//        assertAll(
//            { assertEquals(1, stored.size) },
//            { assertTrue(stored.first().href.endsWith("/77", ignoreCase = true)) }
//        )
//    }
//
//    @Test
//    fun `addLinks(resource, relation, empty toAdd) is no-op`() {
//        val res = ElevfravarResource().apply {
//            linksFor(REL_ELEVFORHOLD).add(link("$REL_ELEVFORHOLD/1"))
//        }
//        updater.addLinks(res, REL_ELEVFORHOLD, emptyList())
//        assertEquals(1, res.linksFor(REL_ELEVFORHOLD).size)
//    }
//
//
//    @Test
//    fun `addLinks(resource, relation, toAdd) creates relation list and adds non-duplicates`() {
//        val res = ElevfravarResource()
//        val toAdd = listOf(link("$REL_ELEVFORHOLD/1"))
//
//        updater.addLinks(res, REL_ELEVFORHOLD, toAdd)
//
//        val stored = res.links[REL_ELEVFORHOLD].orEmpty()
//        assertAll(
//            { assertEquals(1, stored.size) },
//            { assertTrue(stored.first().href.endsWith("$REL_ELEVFORHOLD/1", ignoreCase = true)) }
//        )
//    }
//
//    @Test
//    fun `deleteLinks removes all matches referenced by RelationRef`() {
//        val links = mutableListOf(
//            link("https://test.felleskomponent.no/api/$REL_ELEVFORHOLD/1"),
//            link("https://test.felleskomponent.no/api/$REL_ELEVFORHOLD/2"),
//            link("https://test.felleskomponent.no/api/$REL_ELEVFORHOLD/21")
//        )
//
//        val ref = RelationRef(
//            name = REL_ELEVFORHOLD,
//            links = listOf(link("2"), link("9999"))
//        )
//
//        updater.deleteLinks(links, ref)
//
//        val hrefs = links.hrefs()
//        assertAll(
//            { assertEquals(2, links.size) },
//            { assertTrue(hrefs.any { it.endsWith("/1") }) },
//            { assertTrue(hrefs.any { it.endsWith("/21") }) },
//            { assertFalse(hrefs.any { it.endsWith("/2") }) }
//        )
//    }
//
//    @Test
//    fun `update with ADD adds links to named relation`() {
//        val res = ElevfravarResource().apply {
//            linksFor(REL_ELEVFORHOLD).add(link("$REL_ELEVFORHOLD/1"))
//        }
//
//        val update = RelationUpdate(
//            orgId = "org",
//            domainName = "dom",
//            packageName = "pkg",
//            resource = no.fintlabs.autorelation.model.ResourceRef("Elevforhold", "1"),
//            relation = RelationRef(REL_ELEVFORHOLD, listOf(link("$REL_ELEVFORHOLD/2"))),
//            operation = RelationOperation.ADD
//        )
//
//        updater.update(update, res)
//
//        val stored = res.linksFor(REL_ELEVFORHOLD)
//        val hrefs = stored.hrefs()
//        assertAll(
//            { assertEquals(2, stored.size) },
//            { assertTrue(hrefs.any { it.endsWith("/1") }) },
//            { assertTrue(hrefs.any { it.endsWith("/2") }) }
//        )
//    }
//
//    @Test
//    fun `update with DELETE removes matching links from named relation`() {
//        val res = ElevfravarResource().apply {
//            linksFor(REL_ELEVFORHOLD).apply {
//                add(link("$REL_ELEVFORHOLD/1"))
//                add(link("$REL_ELEVFORHOLD/2"))
//            }
//        }
//
//        val update = RelationUpdate(
//            orgId = "org",
//            domainName = "dom",
//            packageName = "pkg",
//            resource = no.fintlabs.autorelation.model.ResourceRef("Elevforhold", "1"),
//            relation = RelationRef(REL_ELEVFORHOLD, listOf(link("2"))),
//            operation = RelationOperation.DELETE
//        )
//
//        updater.update(update, res)
//
//        val stored = res.linksFor(REL_ELEVFORHOLD)
//        val hrefs = stored.hrefs()
//        assertAll(
//            { assertEquals(1, stored.size) },
//            { assertTrue(hrefs.any { it.endsWith("/1") }) },
//            { assertFalse(hrefs.any { it.endsWith("/2") }) }
//        )
//    }
//
//    @Test
//    fun `attachBuffered is no-op when linksToAttach is empty`() {
//        val res = ElevfravarResource()
//
//        updater.attachBuffered(res, REL_FRAVARSREGISTRERING, emptyList())
//
//        assertFalse(res.links.containsKey(REL_FRAVARSREGISTRERING))
//    }
//
//    @Test
//    fun `attachBuffered adds when linksToAttach is non-empty`() {
//        val res = ElevfravarResource()
//        val newLinks = listOf(
//            link("$REL_FRAVARSREGISTRERING/42"),
//            link("${REL_FRAVARSREGISTRERING.uppercase()}/42") // duplicate by ignoreCase
//        )
//
//        updater.attachBuffered(res, REL_FRAVARSREGISTRERING, newLinks)
//
//        val stored = res.links[REL_FRAVARSREGISTRERING].orEmpty()
//        assertAll(
//            { assertEquals(1, stored.size, "Should add only unique link by endsWith ignoreCase") },
//            { assertTrue(stored.first().href.endsWith("/42", ignoreCase = true)) }
//        )
//    }
//
//    private fun link(href: String) = Link.with(href)
//
//    private fun ElevfravarResource.linksFor(relation: String): MutableList<Link> =
//        links.getOrPut(relation) { mutableListOf() }
//
//    private fun Iterable<Link>.hrefs(): List<String> = map { it.href }
//
//}
