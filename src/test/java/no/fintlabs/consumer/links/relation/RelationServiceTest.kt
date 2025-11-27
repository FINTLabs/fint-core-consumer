package no.fintlabs.consumer.links.relation

import io.mockk.*
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.cache.RelationCache
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationRef
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.autorelation.model.ResourceRef
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class RelationServiceTest {
    private var linkService: LinkService = mockk(relaxed = true)
    private var cacheService: CacheService = mockk(relaxed = true)
    private var unresolvedRelationCache: UnresolvedRelationCache = mockk(relaxed = true)
    private var relationCache: RelationCache = mockk(relaxed = true)
    private var relationUpdater: RelationUpdater = mockk(relaxed = true)
    private var consumerConfig: ConsumerConfiguration = mockk(relaxed = true)
    private var service: RelationService =
        RelationService(unresolvedRelationCache, linkService, cacheService, relationCache, relationUpdater, consumerConfig)

    private val relationUpdate: RelationUpdate = createRelationUpdate()

    @AfterEach
    fun tearDown() = clearAllMocks()

    @Nested
    inner class ProcessRelationUpdateScenarios {
        @Test
        fun `processes when resource exists`() {
            val resource = createElevFravar()

            every {
                cacheService.getCache(relationUpdate.resource.name).get(relationUpdate.resource.id)
            } returns resource

            service.processRelationUpdate(relationUpdate)

            verify(exactly = 1) { relationUpdater.update(relationUpdate, resource) }
            verify(exactly = 1) { linkService.mapLinks(relationUpdate.resource.name, resource) }
        }

        @Test
        fun `buffer link if resource doesn't exist`() {
            every {
                cacheService.getCache(relationUpdate.resource.name).get(relationUpdate.resource.id)
            } returns null

            service.processRelationUpdate(relationUpdate)

            verify(exactly = 1) {
                unresolvedRelationCache.registerRelations(
                    relationUpdate.resource.name,
                    relationUpdate.resource.id,
                    relationUpdate.relation.name,
                    relationUpdate.relation.links,
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

            every { cacheService.getCache(resource).get(resourceId) } returns resourceObject
            every { consumerConfig.domain } returns domain
            every { consumerConfig.packageName } returns pkg
            every { relationCache.inverseRelationsForTarget(domain, pkg, resource) } returns relations

            relations.forEach { relation ->
                every { unresolvedRelationCache.takeRelations(resource, resourceId, relation) } returns links
            }

            service.handleLinks(resource, resourceId, resourceObject)

            verify(exactly = 1) { relationCache.inverseRelationsForTarget(domain, pkg, resource) }

            relations.forEach { relation ->
                verify(exactly = 1) { unresolvedRelationCache.takeRelations(resource, resourceId, relation) }
                verify(exactly = 1) { relationUpdater.attachBuffered(resourceObject, relation, links) }
            }

            confirmVerified(relationCache, unresolvedRelationCache, relationUpdater)
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
            every { relationCache.inverseRelationsForTarget(domain, pkg, resource) } returns emptySet()

            service.handleLinks(resource, resourceId, resourceObject)

            verify(exactly = 1) { relationCache.inverseRelationsForTarget(domain, pkg, resource) }

            verify(exactly = 0) { unresolvedRelationCache.takeRelations(any(), any(), any()) }
            verify(exactly = 0) { relationUpdater.attachBuffered(any(), any(), any()) }

            confirmVerified(relationCache, unresolvedRelationCache, relationUpdater)
        }
    }

    private fun createElevFravar(id: String = "123"): ElevfravarResource =
        ElevfravarResource().apply {
            systemId =
                Identifikator().apply {
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
        operation: RelationOperation = RelationOperation.ADD,
    ) = RelationUpdate(
        orgId = orgId,
        domainName = domain,
        packageName = pkg,
        resource =
            ResourceRef(
                name = resource,
                id = resourceId,
            ),
        relation =
            RelationRef(
                name = relation,
                links = listOf(Link.with("systemid/$relationId")),
            ),
        operation = operation,
    )
}
