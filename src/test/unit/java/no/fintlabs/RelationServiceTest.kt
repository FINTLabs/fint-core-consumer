package no.fintlabs

import io.mockk.*
import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.autorelation.model.*
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.kafka.entity.EntityProducer
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.links.RelationService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertNull

class RelationServiceTest {

    private lateinit var linkService: LinkService
    private lateinit var cacheService: CacheService
    private lateinit var entityProducer: EntityProducer
    private lateinit var consumerConfig: ConsumerConfiguration

    private lateinit var service: RelationService

    private val resource = "elevfravar"
    private val relation = "fravarsregistrering"
    private val orgId = "fintlabs.no"
    private val domain = "utdanning"
    private val pkg = "vurdering"

    @BeforeEach
    fun setUp() {
        linkService = mockk(relaxed = true)
        cacheService = mockk(relaxed = true)
        entityProducer = mockk(relaxed = true)
        consumerConfig = mockk(relaxed = true)


        every { consumerConfig.orgId } returns orgId
        every { consumerConfig.domain } returns domain
        every { consumerConfig.packageName } returns pkg

//        service = RelationService(linkService, cacheService, entityProducer, consumerConfig)
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Nested
    inner class ProcessIfApplicableScenarios {

        @Test
        fun `ignores orgId mismatch`() {
            val update = mockRelationUpdate(orgId = "not-fintlabs")
            val result = service.processIfApplicable(update)
            assertNull(result)
            verify { cacheService wasNot Called }
            verify { entityProducer wasNot Called }
            verify { linkService wasNot Called }
        }

        @Test
        fun `ignores domain mismatch`() {
            val update = mockRelationUpdate(domain = "annet")
            val result = service.processIfApplicable(update)
            assertNull(result)
            verify { cacheService wasNot Called }
            verify { entityProducer wasNot Called }
            verify { linkService wasNot Called }
        }

        @Test
        fun `ignores package mismatch`() {
            val update = mockRelationUpdate(pkg = "annet")
            val result = service.processIfApplicable(update)
            assertNull(result)
            verify { cacheService wasNot Called }
            verify { entityProducer wasNot Called }
            verify { linkService wasNot Called }
        }

        @Test
        fun `processes when everything matches`() {
            val update = mockRelationUpdate()

            every { cacheService.getCache(update.resource.name)?.get(update.resource.id.value) } returns null

            service.processIfApplicable(update)

            verify(exactly = 1) { cacheService.getCache(update.resource.name) }
        }
    }

    @Nested
    inner class ProcessRelationUpdateScenarios {


        // TODO: replace with pooling test when CT-2236 is implemented

        @Test
        fun `stops early when resource is null`() {
            val update = mockRelationUpdate()

            every { cacheService.getCache(update.resource.name)?.get(update.resource.id.value) } returns null

            service.processRelationUpdate(update)

            verify { linkService wasNot Called }
            verify { entityProducer wasNot Called }
        }

        @Test
        fun `processes when resource exists`() {
            val resourceId = "123"
            val update = mockRelationUpdate(resourceId = resourceId)
            val fintResource = createElevFravar(id = resourceId)

            every {
                cacheService.getCache(update.resource.name)?.get(update.resource.id.value)
            } returns fintResource

            service.processRelationUpdate(update)

            verify(exactly = 1) { linkService.mapLinks(update.resource.name, fintResource) }
            verify(exactly = 1) { entityProducer.produceEntity(update, fintResource) }
        }
    }

    private fun createElevFravar(id: String = "123"): ElevfravarResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply {
                identifikatorverdi = id
            }
        }

    private fun mockRelationUpdate(
        orgId: String = this.orgId,
        domain: String = this.domain,
        pkg: String = this.pkg,
        resource: String = this.resource,
        resourceId: String = "123",
        relation: String = this.relation,
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
            entityRetentionTime = null
        )

}