package no.fintlabs.consumer.links.relation

import com.github.benmanes.caffeine.cache.Cache
import no.fint.model.resource.Link
import org.springframework.stereotype.Component

@Component
class UnresolvedRelationCache(
    private val cache: Cache<RelationKey, MutableList<Link>>,
) {
    fun takeRelations(
        resource: String,
        resourceId: String,
        relation: String,
    ): List<Link> =
        cache
            .asMap()
            .remove(RelationKey(resource, resourceId, relation))
            ?.toList()
            .orEmpty()

    fun registerRelations(
        resource: String,
        resourceId: String,
        relation: String,
        relationLinks: List<Link>,
    ) = cache
        .get(RelationKey(resource, resourceId, relation)) { mutableListOf() }
        ?.addAll(relationLinks)
}
