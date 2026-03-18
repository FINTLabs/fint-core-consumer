package no.fintlabs.consumer.resource.dto

import java.util.Date

data class LastUpdatedResponse(
    val lastUpdated: Long,
    val lastFullSync: Date,
)
