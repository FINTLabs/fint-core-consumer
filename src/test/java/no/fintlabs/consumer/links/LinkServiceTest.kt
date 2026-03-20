package no.fintlabs.consumer.links

import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.nested.NestedLinkMapper
import no.fintlabs.consumer.links.nested.NestedLinkService
import no.fintlabs.consumer.resource.context.ResourceContext
import no.fintlabs.consumer.resource.context.ResourceContextCache
import no.fintlabs.reflection.ReflectionCache
import no.fintlabs.reflection.ReflectionInitializer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.elev.ElevResource
import no.novari.fint.model.resource.utdanning.elev.KlasseResource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Import
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig

@SpringJUnitConfig(classes = [LinkServiceTest.Config::class])
@TestPropertySource(
    properties = [
        "fint.consumer.base-url=https://test.felleskomponent.no",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package-name=elev",
        "fint.consumer.org-id=fintlabs.no",
        "fint.consumer.writeable=klasse",
        "fint.consumer.pod-url=http://test",
    ],
)
class LinkServiceTest {
    @EnableConfigurationProperties(ConsumerConfiguration::class)
    @Import(
        ReflectionCache::class,
        ReflectionInitializer::class,
        ResourceContextCache::class,
        ResourceContext::class,
        LinkGenerator::class,
        LinkPaginator::class,
        LinkParser::class,
        NestedLinkMapper::class,
        NestedLinkService::class,
        LinkService::class,
    )
    class Config

    @Autowired
    private lateinit var linkService: LinkService

    private val elevResourceName = "elev"
    private val baseUrl = "https://test.felleskomponent.no"
    private val elevComponentUrl = "$baseUrl/utdanning/elev"
    private val utdanningsprogramUrl = "$baseUrl/utdanning/utdanningsprogram"
    private val elevResourceUrl = "$elevComponentUrl/elev"

    // Self link tests

    @Test
    fun `shouldCreateSelfLinks WhenIdentifikatorExists`() {
        val elevResource = createElevResource("123")
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals("$elevResourceUrl/systemid/123", elevResource.selfLinks.first().href)
    }

    @Test
    fun `shouldResetSelfLinks`() {
        val elevResource = createElevResource("123")
        elevResource.addSelf(Link.with("shouldnt/exist"))
        elevResource.addSelf(Link.with("shouldnt/exist/either"))
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals(1, elevResource.selfLinks.size)
    }

    // Relation link tests

    @Test
    fun `shouldGenerateRelationLink WhenRelationNameIsNotLowercase`() {
        val elevResource = createElevResource("123")
        val linkSegment = "/systemid/123"
        val relationName = "elevfOrhold"
        elevResource.addLink(relationName, Link.with(linkSegment))
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals(
            "$elevComponentUrl/elevforhold/systemid/123",
            elevResource.links[relationName]!!.first().href,
        )
    }

    @Test
    fun `shouldGenerateRelationLink WhenLinkSegmentIsValid with leading slash`() {
        val elevResource = createElevResource("123")
        elevResource.addElevforhold(Link.with("/systemid/123"))
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals("$elevComponentUrl/elevforhold/systemid/123", elevResource.elevforhold.first().href)
    }

    @Test
    fun `shouldGenerateRelationLink WhenLinkSegmentIsValid without leading slash`() {
        val elevResource = createElevResource("123")
        elevResource.addElevforhold(Link.with("systemid/123"))
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals("$elevComponentUrl/elevforhold/systemid/123", elevResource.elevforhold.first().href)
    }

    @Test
    fun `shouldNotProcessLink WhenEntireLinkIsPresent`() {
        val elevResource = createElevResource("123")
        val linkSegment = "https://no-valid-url.com/whatever/ok/systemid/123"
        elevResource.addElevforhold(Link.with(linkSegment))
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals(linkSegment, elevResource.elevforhold.first().href)
    }

    @Test
    fun `shouldGenerateLinkToOtherComponent WhenRelationBelongsToOtherComponent`() {
        val klasseResource = createKlasse("123")
        klasseResource.addSkole(Link.with("systemid/123"))
        linkService.mapLinks("klasse", klasseResource)
        assertEquals("$utdanningsprogramUrl/skole/systemid/123", klasseResource.skole.first().href)
    }

    @Test
    fun `shouldGenerateLink WhenRelationIsCommon`() {
        val elevResource = createElevResource("123")
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals("$elevComponentUrl/person/systemid/123", elevResource.person.first().href)
    }

    // Link behaviour tests

    @Test
    fun `shouldRemoveRelation WhenAllRelationLinksAreNull`() {
        val elevResource = createElevResource("123")
        val relationName = "test"
        elevResource.addLink(relationName, null)
        elevResource.addLink(relationName, null)
        linkService.mapLinks(elevResourceName, elevResource)
        assertNull(elevResource.links[relationName])
    }

    @Test
    fun `shouldRemoveRelation WhenAllRelationLinksHrefAreNull`() {
        val elevResource = createElevResource("123")
        val relationName = "test"
        elevResource.addLink(relationName, Link.with(null as String?))
        elevResource.addLink(relationName, Link.with(null as String?))
        linkService.mapLinks(elevResourceName, elevResource)
        assertNull(elevResource.links[relationName])
    }

    @Test
    fun `shouldRemoveNullLinks`() {
        val elevResource = createElevResource("123")
        val relationName = "test"
        elevResource.addLink(relationName, null)
        elevResource.addLink(relationName, Link.with("123"))
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals(1, elevResource.links[relationName]!!.size)
    }

    @Test
    fun `shouldRemoveLinksWithHrefAsNull`() {
        val elevResource = createElevResource("123")
        val relationName = "test"
        elevResource.addLink(relationName, Link.with(null as String?))
        elevResource.addLink(relationName, Link.with("123"))
        linkService.mapLinks(elevResourceName, elevResource)
        assertEquals(1, elevResource.links[relationName]!!.size)
    }

    private fun createKlasse(id: String): KlasseResource {
        val klasseResource = KlasseResource()
        klasseResource.systemId = Identifikator().apply { identifikatorverdi = id }
        klasseResource.addTrinn(Link.with("systemid/123"))
        klasseResource.addSkole(Link.with("systemid/123"))
        return klasseResource
    }

    private fun createElevResource(id: String): ElevResource {
        val elevResource = ElevResource()
        elevResource.systemId = Identifikator().apply { identifikatorverdi = id }
        elevResource.addPerson(Link.with("systemid/123"))
        return elevResource
    }
}
