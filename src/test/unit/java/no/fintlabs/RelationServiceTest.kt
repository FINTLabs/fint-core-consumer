package no.fintlabs

import io.mockk.*
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.cache.RelationCache
import no.fintlabs.autorelation.model.*
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.links.relation.LinkBuffer
import no.fintlabs.consumer.links.relation.RelationService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class RelationServiceTest {

    private lateinit var linkService: LinkService
    private lateinit var cacheService: CacheService
    private lateinit var linkBuffer: LinkBuffer
    private lateinit var relationCache: RelationCache
    private lateinit var consumerConfig: ConsumerConfiguration
    private lateinit var service: RelationService

    private val relationUpdate: RelationUpdate = createRelationUpdate()

    @BeforeEach
    fun setUp() {
        linkService = mockk(relaxed = true)
        cacheService = mockk(relaxed = true)
        linkBuffer = mockk(relaxed = true)
        relationCache = mockk(relaxed = true)
        consumerConfig = mockk(relaxed = true)

        service = spyk(RelationService(linkService, cacheService, relationCache, consumerConfig, linkBuffer))
    }

    @AfterEach
    fun tearDown() = clearAllMocks()

    @Nested
    inner class ProcessRelationUpdateScenarios {


        @Test
        fun `buffer link if not present`() {
            every { consumerConfig.matchesConfiguration(any(), any(), any()) } returns true
            every { cacheService.getCache(any()).get(any()) } returns createElevFravar()

            service.processIfApplicable(relationUpdate)

            verify(exactly = 1) { linkBuffer.registerLinks(any(), any(), any(), any()) }
        }

        @Test
        fun `processes when resource exists`() {

        }
    }

    private fun createElevFravar(id: String = "123"): ElevfravarResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply {
                identifikatorverdi = id
            }
        }

    private fun createRelationUpdate(
        orgId: String = "fintlabs.no",
        domain: String = "utdanning",
        pkg: String = "vurdering",
        resource: String = "elevfravar",
        resourceId: String = "123",
        relation: String = "fravarsregistrering",
        relationId: String = "321",
        operation: RelationOperation = RelationOperation.ADD
    ) =
        RelationUpdate(
            orgId = orgId,
            domainName = domain,
            packageName = pkg,
            resource = ResourceRef(
                name = resource,
                id = ResourceId("_", resourceId)
            ),
            relation = RelationRef(
                name = relation,
                ids = listOf(ResourceId("_", relationId))
            ),
            operation = operation,
            entityCreatedTime = null
        )

}