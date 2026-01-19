//package no.fintlabs.consumer
//
//import no.novari.fint.model.resource.FintResource
//import no.novari.fint.model.resource.Link
//import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
//import no.fintlabs.adapter.models.sync.SyncType
//import no.fintlabs.autorelation.model.RelationOperation
//import no.fintlabs.autorelation.model.RelationUpdate
//import no.fintlabs.cache.CacheService
//import no.fintlabs.consumer.kafka.entity.ConsumerRecordMetadata
//import no.fintlabs.consumer.kafka.entity.KafkaEntity
//import no.fintlabs.consumer.links.relation.RelationService
//import no.fintlabs.consumer.resource.ResourceService
//import org.junit.jupiter.api.Assertions.assertTrue
//import org.junit.jupiter.api.Test
//import org.junit.jupiter.api.assertNotNull
//import org.junit.jupiter.api.assertNull
//import org.springframework.beans.factory.annotation.Autowired
//import org.springframework.boot.test.context.SpringBootTest
//import org.springframework.kafka.test.context.EmbeddedKafka
//import org.springframework.test.context.ActiveProfiles
//import java.util.*
//import kotlin.test.assertEquals
//
//@SpringBootTest
//@EmbeddedKafka
//@ActiveProfiles("utdanning-vurdering")
//class RelationServiceTest
//    @Autowired
//    constructor(
//        private val cacheService: CacheService,
//        private val relationService: RelationService,
//        private val resourceService: ResourceService,
//    ) {
//        private val resourceName = "elevfravar"
//        private val relationName = "fravarsregistrering"
//        private val resourceId = UUID.randomUUID().toString()
//        private val relationId = UUID.randomUUID().toString()
//
//        @Test
//        fun `Mutate existing resource on relation update event`() {
//            val resource = ElevfravarResource()
//
//            resourceService.processEntityConsumerRecord(createKafkaEntity(resourceId, resource))
//            relationService.processRelationUpdate(createRelationUpdate(RelationOperation.ADD, relationId))
//
//            val fetchedResource = getResource()
//            assertNotNull(fetchedResource)
//            assertRelationIsPresent(fetchedResource)
//        }
//
//        @Test
//        fun `Update new resource with existing resource controlled relations`() {
//            val relationLink = Link.with("systemid/123")
//            val resource =
//                ElevfravarResource().apply {
//                    addFravarsregistrering(relationLink)
//                }
//
//            resourceService.processEntityConsumerRecord(createKafkaEntity(resourceId, resource))
//
//            var fetchedResource = getResource()
//            assertNotNull(fetchedResource)
//
//            val newResource = ElevfravarResource()
//            resourceService.processEntityConsumerRecord(createKafkaEntity(resourceId, newResource))
//
//            fetchedResource = getResource()
//            val firstFravarsRegistreringLink = getFravarsregistreringLinks(fetchedResource).first()
//            assertNotNull(firstFravarsRegistreringLink)
//            assertEquals(relationLink, firstFravarsRegistreringLink)
//        }
//
//        @Test
//        fun `Buffer relation if resource is not present and attach it when resource is present`() {
//            val resource = ElevfravarResource()
//
//            relationService.processRelationUpdate(createRelationUpdate(RelationOperation.ADD, relationId))
//
//            var fetchedResource = getResource()
//            assertNull(fetchedResource)
//
//            resourceService.processEntityConsumerRecord(createKafkaEntity(resourceId, resource))
//
//            fetchedResource = getResource()
//            assertNotNull(fetchedResource)
//            assertRelationIsPresent(fetchedResource)
//        }
//
//        private fun assertRelationIsPresent(resource: FintResource) {
//            val links = getFravarsregistreringLinks(resource)
//            assertEquals(1, links.size)
//            assertTrue(links.first().href.contains(relationId))
//        }
//
//        private fun getFravarsregistreringLinks(resource: FintResource): List<Link> = resource.links[relationName] ?: emptyList()
//
//        private fun getResource() = cacheService.getCache(resourceName).get(resourceId)
//
//        private fun createKafkaEntity(
//            id: String,
//            resource: FintResource,
//            created: Long = System.currentTimeMillis(),
//        ) = KafkaEntity(
//            key = id,
//            resourceName = resourceName,
//            resource = resource,
//            lastModified = created,
//            retentionTime = null,
//            consumerRecordMetadata =
//                ConsumerRecordMetadata(
//                    SyncType.FULL,
//                    id,
//                    1L,
//                ),
//        )
//
//        private fun createRelationUpdate(
//            operation: RelationOperation,
//            vararg relationIds: String,
//        ) = RelationUpdate(
//            orgId = "fintlabs.no",
//            domainName = "utdanning",
//            packageName = "vurdering",
//            resource =
//                ResourceRef(
//                    name = "elevfravar",
//                    id = resourceId,
//                ),
//            relation =
//                RelationRef(
//                    name = "fravarsregistrering",
//                    links = relationIds.map { Link.with("systemid/$it") },
//                ),
//            operation = operation,
//        )
//    }
