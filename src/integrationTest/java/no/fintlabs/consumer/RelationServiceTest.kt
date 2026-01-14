package no.fintlabs.consumer

import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.vurdering.FravarsregistreringResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.entity.ConsumerRecordMetadata
import no.fintlabs.consumer.kafka.entity.KafkaEntity
import no.fintlabs.consumer.links.relation.RelationService
import no.fintlabs.consumer.resource.ResourceService
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.util.*
import kotlin.test.assertEquals

@SpringBootTest
@EmbeddedKafka
@ActiveProfiles("utdanning-vurdering")
class RelationServiceTest
    @Autowired
    constructor(
        private val cacheService: CacheService,
        private val relationService: RelationService,
        private val resourceService: ResourceService,
    ) {
        private val resourceId = UUID.randomUUID().toString()

        @Test
        fun `Mutate existing resource on relation update event`() {
            val resourceName = "fravarsregistrering"
            val resource = FravarsregistreringResource()
            val relationName = "elevfravar"
            val linkToAdd = Link.with("systemid/123")

            val kafkaEntity = createKafkaEntity(resourceId, resourceName, resource)
            resourceService.processKafkaEntity(kafkaEntity)

            val relationUpdate =
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, linkToAdd),
                )
            relationService.processRelationUpdate(relationUpdate)

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertEquals(linkToAdd, links.first())
        }

        @Test
        fun `new resource inherits existing relations`() {
            val resourceName = "fravarsregistrering"
            val relationName = "elevfravar"
            val inheritedLink = Link.with("systemid/123")
            val oldResource =
                FravarsregistreringResource().apply {
                    links[relationName] = mutableListOf(inheritedLink)
                }
            val newResource = FravarsregistreringResource()

            val oldKafkaEntity = createKafkaEntity(resourceId, resourceName, oldResource)
            resourceService.processKafkaEntity(oldKafkaEntity)

            val newKafkaEntity = createKafkaEntity(resourceId, resourceName, newResource)
            resourceService.processKafkaEntity(newKafkaEntity)

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertEquals(inheritedLink, links.first())
        }

        @Test
        fun `Store relation if resource is not present and attach it when resource is present`() {
            val resourceName = "fravarsregistrering"
            val resource = FravarsregistreringResource()
            val relationName = "elevfravar"
            val resourceId = "123"
            val storedLink = Link.with("systemid/123")

            val relationUpdate =
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, storedLink),
                )
            relationService.processRelationUpdate(relationUpdate)

            resourceService.processKafkaEntity(createKafkaEntity(resourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertEquals(storedLink, links.first())
        }

        @Test
        fun `Delete existing relation on relation update event`() {
            val resourceName = "fravarsregistrering"
            val relationName = "elevfravar"
            val linkToDelete = Link.with("systemid/123")

            val resource =
                FravarsregistreringResource().apply {
                    addLink(relationName, linkToDelete)
                }

            val kafkaEntity = createKafkaEntity(resourceId, resourceName, resource)
            resourceService.processKafkaEntity(kafkaEntity)

            val relationUpdate =
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.DELETE,
                    RelationBinding(relationName, linkToDelete),
                )

            relationService.processRelationUpdate(relationUpdate)

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            kotlin.test.assertNull(links, "The relation key should be removed when the last link is deleted")
        }

        private fun createKafkaEntity(
            id: String,
            resourceName: String,
            resource: FintResource,
            created: Long = System.currentTimeMillis(),
        ) = KafkaEntity(
            key = id,
            resourceName = resourceName,
            resource = resource,
            lastModified = created,
            retentionTime = null,
            consumerRecordMetadata =
                ConsumerRecordMetadata(
                    SyncType.FULL,
                    id,
                    1L,
                ),
        )

        private fun createRelationUpdate(
            resourceName: String,
            resourceId: String,
            operation: RelationOperation,
            relationBinding: RelationBinding,
        ) = RelationUpdate(
            orgId = "fintlabs.no",
            targetEntity = EntityDescriptor("utdanning", "vurdering", resourceName),
            targetId = resourceId,
            binding = relationBinding,
            operation = operation,
        )
    }
