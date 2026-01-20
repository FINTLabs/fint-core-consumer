package no.fintlabs.autorelation

import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link

/**
 * Identifies links that existed in the [oldResource] but are missing in the current [this] resource
 * for the specified [relations].
 *
 * @return A map where the key is the relation name and the value is the list of links to be deleted.
 */
fun FintResource.findObsoleteLinks(
    oldResource: FintResource,
    relations: Collection<String>,
): Map<String, List<Link>> {
    val obsoleteLinks = mutableMapOf<String, List<Link>>()

    relations.forEach { relation ->
        val oldLinks = oldResource.links[relation]
        val newLinks = links[relation]

        if (!oldLinks.isNullOrEmpty()) {
            val missing =
                if (newLinks.isNullOrEmpty()) {
                    oldLinks
                } else {
                    oldLinks.filter { old -> newLinks.none { new -> old.isSameResource(new) } }
                }

            if (missing.isNotEmpty()) {
                obsoleteLinks[relation] = missing
            }
        }
    }
    return obsoleteLinks
}

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

private fun Link.linkMatches(link: Link): Boolean = link.href.endsWith(this.href, ignoreCase = true)
