package no.fintlabs.consumer.links.relation

import io.mockk.*
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
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
    private var relationRuleRegistry: RelationRuleRegistry = mockk(relaxed = true)
    private var consumerConfig: ConsumerConfiguration = mockk(relaxed = true)
    private var service: RelationService =
        RelationService(unresolvedRelationCache, linkService, cacheService, relationRuleRegistry, consumerConfig)

    private val relationUpdate: RelationUpdate = createRelationUpdate()

    @AfterEach
    fun tearDown() = clearAllMocks()

    @Nested
    inner class ProcessRelationUpdateScenarios {
        @Test
        fun `processes when resource exists`() {
            val resource = createElevFravar()

            every {
                cacheService.getCache(relationUpdate.targetEntity.resourceName).get(relationUpdate.targetId)
            } returns resource

            service.processRelationUpdate(relationUpdate)

            verify(exactly = 1) { linkService.mapLinks(relationUpdate.targetEntity.resourceName, resource) }
        }

        @Test
        fun `buffer link if resource doesn't exist`() {
            every {
                cacheService.getCache(relationUpdate.targetEntity.resourceName).get(relationUpdate.targetId)
            } returns null

            service.processRelationUpdate(relationUpdate)

            verify(exactly = 1) {
                unresolvedRelationCache.registerRelations(
                    relationUpdate.targetEntity.resourceName,
                    relationUpdate.targetId,
                    relationUpdate.binding.relationName,
                    relationUpdate.binding.link,
                )
            }

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
            every { relationRuleRegistry.getInverseRelations(domain, pkg, resource) } returns relations

            relations.forEach { relation ->
                every { unresolvedRelationCache.takeRelations(resource, resourceId, relation) } returns links
            }

            service.attachRelations(resource, resourceId, resourceObject)

            verify(exactly = 1) { relationRuleRegistry.getInverseRelations(domain, pkg, resource) }

            relations.forEach { relation ->
                verify(exactly = 1) { unresolvedRelationCache.takeRelations(resource, resourceId, relation) }
            }

            confirmVerified(relationRuleRegistry, unresolvedRelationCache)
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
            every { relationRuleRegistry.getInverseRelations(domain, pkg, resource) } returns emptySet()

            service.attachRelations(resource, resourceId, resourceObject)

            verify(exactly = 1) { relationRuleRegistry.getInverseRelations(domain, pkg, resource) }

            verify(exactly = 0) { unresolvedRelationCache.takeRelations(any(), any(), any()) }

            confirmVerified(relationRuleRegistry, unresolvedRelationCache)
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
        binding =
            RelationBinding(
                relationName = relation,
                link = Link.with("systemid/$relationId"),
            ),
        operation = operation,
        targetEntity = EntityDescriptor(domain, pkg, resource),
        targetId = resourceId,
    )
}
