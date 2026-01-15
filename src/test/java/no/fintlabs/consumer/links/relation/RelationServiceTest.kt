package no.fintlabs.consumer.links.relation

import io.mockk.*
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.*
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.event.RelationEventProducer
import no.fintlabs.consumer.links.LinkService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class RelationServiceTest {
    private var linkService: LinkService = mockk(relaxed = true)
    private var cacheService: CacheService = mockk(relaxed = true)
    private var pendingRelationCache: UnresolvedRelationCache = mockk(relaxed = true)
    private var relationRuleRegistry: RelationRuleRegistry = mockk(relaxed = true)
    private var consumerConfig: ConsumerConfiguration = mockk(relaxed = true)
    private var relationEventProducer: RelationEventProducer = mockk(relaxed = true)

    private var service: RelationService =
        RelationService(
            pendingRelationCache,
            linkService,
            cacheService,
            relationRuleRegistry,
            consumerConfig,
            relationEventProducer,
        )

    private val relationUpdate: RelationUpdate = createRelationUpdate()

    @AfterEach
    fun tearDown() = clearAllMocks()

    @Nested
    inner class ProcessRelationUpdateScenarios {
        @Test
        fun `should apply update immediately when resource exists in cache`() {
            val resource = createElevFravar()

            every {
                cacheService.getCache(relationUpdate.targetEntity.resourceName).get(relationUpdate.targetId)
            } returns resource

            service.applyOrBufferUpdate(relationUpdate)

            verify(exactly = 1) { linkService.mapLinks(relationUpdate.targetEntity.resourceName, resource) }
            verify { pendingRelationCache wasNot Called }
        }

        @Test
        fun `should buffer ADD operation if resource does not exist`() {
            val addUpdate = createRelationUpdate(operation = RelationOperation.ADD)

            every {
                cacheService.getCache(addUpdate.targetEntity.resourceName).get(addUpdate.targetId)
            } returns null

            service.applyOrBufferUpdate(addUpdate)

            verify(exactly = 1) {
                pendingRelationCache.registerRelation(
                    resourceName = addUpdate.targetEntity.resourceName,
                    resourceId = addUpdate.targetId,
                    relationName = addUpdate.binding.relationName,
                    relationLink = addUpdate.binding.link,
                )
            }
        }

        @Test
        fun `should buffer DELETE operation if resource does not exist`() {
            val deleteUpdate = createRelationUpdate(operation = RelationOperation.DELETE)

            every {
                cacheService.getCache(deleteUpdate.targetEntity.resourceName).get(deleteUpdate.targetId)
            } returns null

            service.applyOrBufferUpdate(deleteUpdate)

            verify(exactly = 1) {
                pendingRelationCache.removeRelation(
                    resourceName = deleteUpdate.targetEntity.resourceName,
                    resourceId = deleteUpdate.targetId,
                    relationName = deleteUpdate.binding.relationName,
                    relationLink = deleteUpdate.binding.link,
                )
            }
        }
    }

    @Nested
    inner class RaceConditionScenarios {
        @Test
        fun `should apply update if resource arrives during buffering (Double Check)`() {
            val lateArrivingResource = createElevFravar()

            // First call returns null (simulate missing).
            // Second call returns resource (simulate arrival during buffering).
            every {
                cacheService.getCache(relationUpdate.targetEntity.resourceName).get(relationUpdate.targetId)
            } returnsMany listOf(null, lateArrivingResource)

            service.applyOrBufferUpdate(relationUpdate)

            // Verify buffering still happened (because first check failed)
            verify(exactly = 1) {
                pendingRelationCache.registerRelation(any(), any(), any(), any())
            }

            // Verify the update was applied to the late-arriving resource (because second check succeeded)
            verify(exactly = 1) {
                linkService.mapLinks(relationUpdate.targetEntity.resourceName, lateArrivingResource)
            }
        }
    }

    @Nested
    inner class ReconcileLinksScenarios {
        @Test
        fun `should handle new resource (no old resource) without pruning`() {
            val resourceName = "elevfravar"
            val resourceId = "123"
            val newResource = createElevFravar(resourceId)

            every { consumerConfig.domain } returns "test-domain"
            every { consumerConfig.packageName } returns "test-pkg"
            every { cacheService.getCache(resourceName).get(resourceId) } returns null

            service.reconcileLinks(resourceName, resourceId, newResource)

            verify(exactly = 0) { relationEventProducer.publish(any()) }
            verify(exactly = 1) { relationRuleRegistry.getInverseRelations(any(), any(), any()) }
        }

        @Test
        fun `should publish DELETE event when links are removed (Pruning)`() {
            val resourceName = "elevfravar"
            val resourceId = "123"
            val relationName = "rel_test"
            val orgId = "fintlabs.no"

            val oldResource =
                createElevFravar(resourceId).apply {
                    addLink(relationName, Link.with("http://old-link"))
                }

            val newResource = createElevFravar(resourceId)

            every { consumerConfig.domain } returns "test-domain"
            every { consumerConfig.packageName } returns "test-pkg"
            every { consumerConfig.orgId } returns orgId
            every {
                relationRuleRegistry.getRules("test-domain", "test-pkg", resourceName)
            } returns listOf(mockk { every { targetRelation } returns relationName })
            every { cacheService.getCache(resourceName).get(resourceId) } returns oldResource

            service.reconcileLinks(resourceName, resourceId, newResource)

            val eventSlot = slot<RelationEvent>()
            verify(exactly = 1) { relationEventProducer.publish(capture(eventSlot)) }

            val event = eventSlot.captured
            assertEquals(RelationOperation.DELETE, event.operation)
            assertEquals(oldResource, event.sourceData)
        }

        @Test
        fun `should preserve links from old resource if configured`() {
            val resourceName = "elevfravar"
            val resourceId = "123"
            val relationName = "managed_relation"
            val oldLink = Link.with("http://should-be-kept")

            val oldResource =
                createElevFravar(resourceId).apply {
                    addLink(relationName, oldLink)
                }
            val newResource = createElevFravar(resourceId)

            every { consumerConfig.domain } returns "test-domain"
            every { consumerConfig.packageName } returns "test-pkg"
            every { cacheService.getCache(resourceName).get(resourceId) } returns oldResource
            every {
                relationRuleRegistry.getInverseRelations("test-domain", "test-pkg", resourceName)
            } returns setOf(relationName)

            service.reconcileLinks(resourceName, resourceId, newResource)

            assert(newResource.links[relationName]?.contains(oldLink) == true)
        }

        @Test
        fun `should apply pending links from buffer`() {
            val resourceName = "elevfravar"
            val resourceId = "123"
            val relationName = "managed_relation"
            val pendingLink = Link.with("http://pending-link")

            val newResource = createElevFravar(resourceId)

            every { consumerConfig.domain } returns "test-domain"
            every { consumerConfig.packageName } returns "test-pkg"
            every { cacheService.getCache(resourceName).get(resourceId) } returns null
            every {
                relationRuleRegistry.getInverseRelations("test-domain", "test-pkg", resourceName)
            } returns setOf(relationName)
            every {
                pendingRelationCache.takeRelations(resourceName, resourceId, relationName)
            } returns listOf(pendingLink)

            service.reconcileLinks(resourceName, resourceId, newResource)

            assert(newResource.links[relationName]?.contains(pendingLink) == true)
        }
    }

    private fun createElevFravar(id: String = "123"): ElevfravarResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
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