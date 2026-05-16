package no.fintlabs.consumer.health

data class BootstrapReadinessSnapshot(
    val ready: Boolean,
    val blockingListeners: List<ListenerBootstrapStatus>,
)
