package no.fintlabs.cache

import no.novari.fint.model.resource.FintResource

interface FintCache {
    val lastUpdated: Long
    val size: Int

    fun put(
        resourceId: String,
        resource: FintResource,
        timestamp: Long,
    ): Boolean

    fun get(resourceId: String): FintResource?

    fun lastUpdatedByResourceId(resourceId: String): Long?

    fun getByIdField(
        field: String,
        value: Any,
    ): FintResource?

    fun getList(
        size: Long,
        offset: Long,
        sinceTimestamp: Long,
        filter: String?,
    ): List<FintResource>

    fun remove(
        resourceId: String,
        timestamp: Long,
    )

    fun evictExpired(timestamp: Long): Set<Pair<String, FintResource>>
}
