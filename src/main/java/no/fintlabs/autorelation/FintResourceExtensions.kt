package no.fintlabs.autorelation

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link

/**
 * Creates a complete deep copy of the FintResource to prevent mutating cached instances.
 * Requires the Spring-managed ObjectMapper.
 */
inline fun <reified T : FintResource> T.deepCopy(objectMapper: ObjectMapper): T =
    objectMapper
        .writeValueAsBytes(this)
        .run { objectMapper.readValue<T>(this) }

/**
 * Compares [this] (new resource) with [oldResource] and returns a map of
 * relation names to links that existed in the old resource but are missing in the new one.
 */
fun FintResource.findObsoleteLinks(
    oldResource: FintResource,
    managedRelations: List<String>,
): Map<String, List<Link>> =
    managedRelations
        .associateWith { relationName ->
            val currentLinks = this.links[relationName] ?: emptyList()
            val oldLinks = oldResource.links[relationName] ?: emptyList()

            oldLinks.filter { oldLink ->
                currentLinks.none { currentLink -> currentLink.isSameResource(oldLink) }
            }
        }.filterValues { it.isNotEmpty() }

/**
 * Checks if two links refer to the same resource by comparing the last two segments of their HREF (ID/Value).
 * Safe to use even if hrefs are null.
 */
fun Link.isSameResource(other: Link): Boolean {
    if (this.href == other.href) return true

    val mySuffix = this.getIdSuffix() ?: return false
    val otherSuffix = other.getIdSuffix() ?: return false

    return mySuffix.equals(otherSuffix, ignoreCase = true)
}

private fun Link.getIdSuffix(): String? {
    val url = this.href ?: return null

    val segments = url.split("/").filter { it.isNotBlank() }

    return if (segments.size >= 2) {
        "${segments[segments.size - 2]}/${segments.last()}"
    } else {
        null
    }
}

/**
 * Mutates this resource by applying the given [relationUpdate] (ADD or DELETE).
 * Returns the resource itself for chaining.
 */
fun FintResource.applyUpdate(relationUpdate: RelationUpdate): FintResource {
    val relation = relationUpdate.binding.relationName
    val link = relationUpdate.binding.link

    when (relationUpdate.operation) {
        RelationOperation.ADD -> getRelationLinks(relation).addUniqueLink(link)
        RelationOperation.DELETE -> removeRelationLink(relation, link)
    }
    return this
}

/**
 * Adds [linksToAttach] to the specified [relation], ensuring no duplicates are created.
 * Ignores the request if the list is empty.
 */
fun FintResource.addUniqueLinks(
    relation: String,
    linksToAttach: List<Link>,
): FintResource {
    if (linksToAttach.isNotEmpty()) {
        getRelationLinks(relation).addUniqueLinks(linksToAttach)
    }
    return this
}

/**
 * Removes a specific [link] from the [relation].
 * If the relation list becomes empty after removal, the relation key is removed entirely.
 * Does nothing if the relation does not exist.
 */
fun FintResource.removeRelationLink(
    relation: String,
    link: Link,
) {
    links[relation]?.let { targetList ->
        targetList.removeMatchingLink(link)
        if (targetList.isEmpty()) {
            links.remove(relation)
        }
    }
}

/**
 * Retrieves the list of links for the given [relation], creating a new list if none exists.
 */
fun FintResource.getRelationLinks(relation: String): MutableList<Link> = links.getOrPut(relation) { mutableListOf() }

/**
 * Adds multiple links to the list, skipping any that already exist.
 */
fun MutableList<Link>.addUniqueLinks(links: List<Link>) = links.forEach { addUniqueLink(it) }

/**
 * Adds [link] to the list only if a matching link (based on href suffix) does not already exist.
 */
fun MutableList<Link>.addUniqueLink(link: Link): Boolean = this.none { it.linkMatches(link) } && add(link)

/**
 * Removes any link from the list that matches the given [link] (based on href suffix).
 */
fun MutableList<Link>.removeMatchingLink(link: Link): Boolean = removeIf { it.linkMatches(link) }

private fun Link.linkMatches(link: Link): Boolean =
    this.href.endsWith(link.href, ignoreCase = true) ||
        link.href.endsWith(this.href, ignoreCase = true)
