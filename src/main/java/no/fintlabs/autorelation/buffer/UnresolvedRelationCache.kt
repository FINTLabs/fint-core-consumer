package no.fintlabs.autorelation.buffer

import com.github.benmanes.caffeine.cache.Cache
import no.novari.fint.model.resource.Link
import org.springframework.stereotype.Component

@Component
class UnresolvedRelationCache(
    private val cache: Cache<RelationKey, MutableList<Link>>,
) {
    fun takeRelations(
        resourceName: String,
        resourceId: String,
        relationName: String,
    ): List<Link> =
        cache
            .asMap()
            .remove(RelationKey(resourceName, resourceId, relationName))
            ?.toList()
            .orEmpty()

    fun registerRelation(
        resourceName: String,
        resourceId: String,
        relationName: String,
        relationLink: Link,
    ) = cache
        .get(RelationKey(resourceName, resourceId, relationName)) { mutableListOf() }
        ?.add(relationLink)

    fun removeRelation(
        resourceName: String,
        resourceId: String,
        relationName: String,
        relationLink: Link,
    ) = cache
        .get(RelationKey(resourceName, resourceId, relationName)) { mutableListOf() }
        ?.remove(relationLink)
}
