package no.fintlabs.autorelation

import io.mockk.mockk
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.config.MongoTestcontainerInitializer
import no.novari.fint.model.resource.Link
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import kotlin.test.assertEquals

/**
 * Exercises the Mongo-backed [UnresolvedRelationCache] against a Testcontainers Mongo instance.
 */
class UnresolvedRelationCacheTest {
    private lateinit var mongoTemplate: MongoTemplate
    private lateinit var buffer: UnresolvedRelationCache

    private val targetType = "fravarsregistrering"
    private val targetId = "FR-1"
    private val relation = "elevfravar"

    @BeforeEach
    fun setUp() {
        val factory =
            SimpleMongoClientDatabaseFactory(
                MongoTestcontainerInitializer.MONGO.getReplicaSetUrl("fintcache-buffer"),
            )
        mongoTemplate = MongoTemplate(factory)
        mongoTemplate.dropCollection(UnresolvedRelationCache.COLLECTION)
        buffer = UnresolvedRelationCache(mongoTemplate, mockk(relaxed = true))
    }

    @Test
    fun `registerRelation stores link and takeRelations retrieves it`() {
        buffer.registerRelation(targetType, targetId, relation, Link.with("systemid/abc123"))

        assertEquals(
            listOf("systemid/abc123"),
            buffer.takeRelations(targetType, targetId, relation).mapNotNull { it.href },
        )
    }

    @Test
    fun `takeRelations clears the entry`() {
        buffer.registerRelation(targetType, targetId, relation, Link.with("systemid/abc123"))
        buffer.takeRelations(targetType, targetId, relation)

        assertEquals(emptyList(), buffer.takeRelations(targetType, targetId, relation))
    }

    @Test
    fun `distinct links are retained, the same link is idempotent`() {
        buffer.registerRelation(targetType, targetId, relation, Link.with("systemid/abc123"))
        buffer.registerRelation(targetType, targetId, relation, Link.with("systemid/def456"))
        buffer.registerRelation(targetType, targetId, relation, Link.with("systemid/abc123"))

        assertEquals(
            setOf("systemid/abc123", "systemid/def456"),
            buffer.takeRelations(targetType, targetId, relation).mapNotNull { it.href }.toSet(),
        )
    }

    @Test
    fun `removeRelation removes the specific pending link`() {
        buffer.registerRelation(targetType, targetId, relation, Link.with("systemid/abc123"))
        buffer.registerRelation(targetType, targetId, relation, Link.with("systemid/def456"))

        buffer.removeRelation(targetType, targetId, relation, Link.with("systemid/abc123"))

        assertEquals(
            listOf("systemid/def456"),
            buffer.takeRelations(targetType, targetId, relation).mapNotNull { it.href },
        )
    }

    @Test
    fun `findPendingTargets returns target ids with a pending link to the given source`() {
        buffer.registerRelation(targetType, "FR-1", relation, Link.with("systemid/abc123"))
        buffer.registerRelation(targetType, "FR-2", relation, Link.with("systemid/abc123"))
        buffer.registerRelation(targetType, "FR-3", relation, Link.with("systemid/other"))

        assertEquals(setOf("FR-1", "FR-2"), buffer.findPendingTargets(targetType, relation, "systemid/abc123"))
    }

    @Test
    fun `findPendingTargets matches the same normalization as a resolved link`() {
        buffer.registerRelation(
            targetType,
            "FR-1",
            relation,
            Link.with("https://api.felleskomponent.no/utdanning/vurdering/elevfravar/SystemId/abc123"),
        )

        assertEquals(setOf("FR-1"), buffer.findPendingTargets(targetType, relation, "systemid/abc123"))
    }
}
