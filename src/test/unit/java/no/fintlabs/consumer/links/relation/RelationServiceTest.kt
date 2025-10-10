package no.fintlabs.consumer.links.relation

import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.cache.RelationCache
import no.fintlabs.autorelation.model.*
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class RelationServiceTest {

    private var linkService: LinkService = mockk(relaxed = true)
    private var cacheService: CacheService = mockk(relaxed = true)
    private var linkBuffer: LinkBuffer = mockk(relaxed = true)
    private var relationCache: RelationCache = mockk(relaxed = true)
    private var relationUpdater: RelationUpdater = mockk(relaxed = true)
    private var consumerConfig: ConsumerConfiguration = mockk(relaxed = true)
    private var service: RelationService =
        RelationService(linkBuffer, linkService, cacheService, relationCache, relationUpdater, consumerConfig)

    private val relationUpdate: RelationUpdate = createRelationUpdate()

    @AfterEach
    fun tearDown() = clearAllMocks()

    @Nested
    inner class ProcessRelationUpdateScenarios {

        @Test
        fun `processes when resource exists`() {
            val resource = createElevFravar()

            every {
                cacheService.getCache(relationUpdate.resource.name).get(relationUpdate.resource.id.value)
            } returns resource

            service.processRelationUpdate(relationUpdate)

            verify(exactly = 1) { relationUpdater.update(relationUpdate, resource) }
            verify(exactly = 1) { linkService.mapLinks(relationUpdate.resource.name, resource) }
        }

        @Test
        fun `buffer link if resource doesn't exist`() {
            every {
                cacheService.getCache(relationUpdate.resource.name).get(relationUpdate.resource.id.value)
            } returns null

            service.processRelationUpdate(relationUpdate)

            verify(exactly = 1) {
                linkBuffer.registerLinks(
                    relationUpdate.resource.name,
                    relationUpdate.resource.id.value,
                    relationUpdate.relation.name,
                    relationUpdate.relation.createLinks()
                )
            }

            verify(exactly = 0) { relationUpdater.update(any(), any()) }
            verify(exactly = 0) { linkService.mapLinks(any(), any()) }
        }
    }

    @Nested
    inner class AttachBufferedRelationsScenarios {

        @Test
        fun `buffers links for each controlled relation`() {
            val resource = "elevfravar"
            val resourceId = "123"
            val relations = setOf("relA", "relB")
            val links = listOf(Link.with("linkA"), Link.with("linkB"))
            val resourceObject = createElevFravar()

            val domain = "utdanning"
            val pkg = "vurdering"

            every { consumerConfig.domain } returns domain
            every { consumerConfig.packageName } returns pkg
            every { relationCache.getControlledRelationsForTarget(domain, pkg, resource) } returns relations

            relations.forEach { relation ->
                every { linkBuffer.pollLinks(resource, resourceId, relation) } returns links
            }

            service.attachBufferedRelations(resource, resourceId, resourceObject)

            verify(exactly = 1) { relationCache.getControlledRelationsForTarget(domain, pkg, resource) }

            relations.forEach { relation ->
                verify(exactly = 1) { linkBuffer.pollLinks(resource, resourceId, relation) }
                verify(exactly = 1) { relationUpdater.attachBuffered(resourceObject, relation, links) }
            }

            confirmVerified(relationCache, linkBuffer, relationUpdater)
        }

        @Test
        fun `does nothing when there are no controlled relations`() {
            val resource = "elevfravar"
            val resourceId = "123"
            val resourceObject = createElevFravar()

            val domain = "utdanning"
            val pkg = "vurdering"

            every { consumerConfig.domain } returns domain
            every { consumerConfig.packageName } returns pkg
            every { relationCache.getControlledRelationsForTarget(domain, pkg, resource) } returns emptySet()

            val result = service.attachBufferedRelations(resource, resourceId, resourceObject)

            verify(exactly = 1) { relationCache.getControlledRelationsForTarget(domain, pkg, resource) }

            verify(exactly = 0) { linkBuffer.pollLinks(any(), any(), any()) }
            verify(exactly = 0) { relationUpdater.attachBuffered(any(), any(), any()) }

            assert(result.isEmpty())

            confirmVerified(relationCache, linkBuffer, relationUpdater)
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