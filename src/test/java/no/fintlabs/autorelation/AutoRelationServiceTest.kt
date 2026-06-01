package no.fintlabs.autorelation

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.InvalidLinkException
import no.fintlabs.autorelation.model.MetricReason
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationState
import no.fintlabs.cache.CacheService
import no.fintlabs.cache.FintCache
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceLockService
import no.fintlabs.consumer.resource.context.ResourceContext
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class AutoRelationServiceTest {
    private val linkService: LinkService = mockk(relaxed = true)
    private val cacheService: CacheService = mockk(relaxed = true)
    private val cache: FintCache = mockk(relaxed = true)
    private val unresolvedRelationCache: UnresolvedRelationCache = mockk(relaxed = true)
    private val relationRuleRegistry: RelationRuleRegistry = mockk(relaxed = true)
    private val consumerConfig: ConsumerConfiguration = mockk(relaxed = true)
    private val relationEventService: RelationEventService = mockk(relaxed = true)
    private val objectMapper: ObjectMapper = jacksonObjectMapper()
    private val resourceContext: ResourceContext = mockk(relaxed = true)
    private val resourceLockService: ResourceLockService =
        mockk {
            every { withLock(any(), any(), any()) } answers {
                thirdArg<() -> Unit>()()
            }
        }
    private val metricService: MetricService = mockk(relaxed = true)

    private val service =
        AutoRelationService(
            linkService,
            cacheService,
            consumerConfig,
            relationRuleRegistry,
            relationEventService,
            unresolvedRelationCache,
            resourceContext,
            objectMapper,
            resourceLockService,
            metricService,
        )

    private val resourceName = "elevfravar"
    private val inverseRelation = "fravarsregistrering"

    @BeforeEach
    fun setUp() {
        every { cacheService.getCache(any()) } returns cache
        every { resourceContext.getResource(any())!!.clazz } returns
            ElevfravarResource::class.java as Class<out FintResource>
        every { cache.put(any(), any(), any()) } returns true
    }

    @AfterEach
    fun tearDown() = clearAllMocks()

    @Nested
    inner class ProcessStateScenarios {
        @Test
        fun `adds a back-link to a cached target not yet pointing back`() {
            every { cache.get("t1") } returns createElevFravar("t1")

            service.process(state(listOf("t1")))

            verify(exactly = 1) { cache.put("t1", any(), any()) }
            verify(exactly = 1) { metricService.incrementUpdateApplied(resourceName, "added") }
            verify(exactly = 0) { unresolvedRelationCache.registerRelation(any(), any(), any(), any(), any()) }
        }

        @Test
        fun `removes a back-link from a resolved target no longer in the state`() {
            every { cache.findIdsByRelationLink(inverseRelation, "systemid/source-1") } returns setOf("t1", "t2")
            every { cache.get(any()) } returns createElevFravar("t2")

            service.process(state(listOf("t1")))

            verify(exactly = 1) { cache.put("t2", any(), any()) }
            verify(exactly = 1) { metricService.incrementUpdateApplied(resourceName, "removed") }
            verify(exactly = 0) { metricService.incrementUpdateApplied(resourceName, "added") }
        }

        @Test
        fun `buffers an add when the target is not cached`() {
            every { cache.get("t1") } returns null

            service.process(state(listOf("t1")))

            verify(exactly = 1) {
                unresolvedRelationCache.registerRelation(resourceName, "t1", inverseRelation, any(), any())
            }
            verify(exactly = 1) { metricService.incrementUpdateBuffered(resourceName) }
            verify(exactly = 0) { cache.put(any(), any(), any()) }
        }

        @Test
        fun `drops a buffered pending target no longer in the state`() {
            every {
                unresolvedRelationCache.findPendingTargets(
                    resourceName,
                    inverseRelation,
                    "systemid/source-1",
                )
            } returns
                setOf("t1")

            service.process(state(emptyList()))

            verify(exactly = 1) { unresolvedRelationCache.removeRelation(resourceName, "t1", inverseRelation, any()) }
        }

        @Test
        fun `leaves an already-resolved, still-desired target untouched`() {
            every { cache.findIdsByRelationLink(inverseRelation, "systemid/source-1") } returns setOf("t1")

            service.process(state(listOf("t1")))

            verify(exactly = 0) { cache.put(any(), any(), any()) }
            verify(exactly = 0) { unresolvedRelationCache.registerRelation(any(), any(), any(), any(), any()) }
            verify(exactly = 0) { unresolvedRelationCache.removeRelation(any(), any(), any(), any()) }
        }

        @Test
        fun `records cache put rejected when put returns false`() {
            every { cache.get("t1") } returns createElevFravar("t1")
            every { cache.put(any(), any(), any()) } returns false

            service.process(state(listOf("t1")))

            verify(exactly = 1) { metricService.incrementCachePutRejectedOlderTimestamp(resourceName) }
        }

        @Test
        fun `records UNEXPECTED_ERROR when applying throws an unexpected exception`() {
            every { cache.get("t1") } returns createElevFravar("t1")
            every { linkService.mapLinks(resourceName, any()) } throws RuntimeException("boom")

            service.process(state(listOf("t1")))

            verify(exactly = 1) { metricService.incrementUpdateFailed(resourceName, MetricReason.UNEXPECTED_ERROR) }
            verify(exactly = 0) { metricService.incrementUpdateApplied(any(), any()) }
        }

        @Test
        fun `records the metric reason when applying throws an AutoRelationException`() {
            every { cache.get("t1") } returns createElevFravar("t1")
            every { linkService.mapLinks(resourceName, any()) } throws InvalidLinkException("rel")

            service.process(state(listOf("t1")))

            verify(exactly = 1) { metricService.incrementUpdateFailed(resourceName, MetricReason.INVALID_LINK) }
        }
    }

    @Nested
    inner class ReconcileLinksScenarios {
        @Test
        fun `with no old resource it reads inverse relations and does not fail`() {
            every { consumerConfig.domain } returns "test-domain"
            every { consumerConfig.packageName } returns "test-pkg"
            every { cache.get("123") } returns null

            service.reconcileLinks(resourceName, "123", createElevFravar("123"))

            verify(exactly = 1) { relationRuleRegistry.getInverseRelations(any(), any(), any()) }
        }

        @Test
        fun `preserves inverse links from the old resource`() {
            val relation = "managed_relation"
            val oldLink = Link.with("http://should-be-kept")
            val oldResource = createElevFravar("123").apply { addLink(relation, oldLink) }
            val newResource = createElevFravar("123")

            every { consumerConfig.domain } returns "test-domain"
            every { consumerConfig.packageName } returns "test-pkg"
            every { cache.get("123") } returns oldResource
            every { relationRuleRegistry.getRules("test-domain", "test-pkg", resourceName) } returns emptyList()
            every { relationRuleRegistry.getInverseRelations("test-domain", "test-pkg", resourceName) } returns
                setOf(relation)

            service.reconcileLinks(resourceName, "123", newResource)

            assert(newResource.links[relation]?.contains(oldLink) == true)
            verify(exactly = 1) { metricService.incrementPreservedLinks(resourceName, relation, 1) }
        }

        @Test
        fun `hydrates pending links from the buffer`() {
            val relation = "managed_relation"
            val pendingLink = Link.with("http://pending-link")
            val newResource = createElevFravar("123")

            every { consumerConfig.domain } returns "test-domain"
            every { consumerConfig.packageName } returns "test-pkg"
            every { cache.get("123") } returns null
            every { relationRuleRegistry.getRules("test-domain", "test-pkg", resourceName) } returns emptyList()
            every { relationRuleRegistry.getInverseRelations("test-domain", "test-pkg", resourceName) } returns
                setOf(relation)
            every { unresolvedRelationCache.takeRelations(resourceName, "123", relation) } returns listOf(pendingLink)

            service.reconcileLinks(resourceName, "123", newResource)

            assert(newResource.links[relation]?.contains(pendingLink) == true)
            verify(exactly = 1) { metricService.incrementHydratedLinks(resourceName, relation, 1) }
        }
    }

    private fun state(
        targetIds: List<String>,
        relation: String = inverseRelation,
        sourceLink: String = "systemid/source-1",
    ) = RelationState(
        targetEntity = EntityDescriptor("utdanning", "vurdering", resourceName),
        targetIds = targetIds,
        binding = RelationBinding(relation, Link.with(sourceLink)),
        timestamp = 1000L,
    )

    private fun createElevFravar(id: String = "123"): ElevfravarResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }
}
