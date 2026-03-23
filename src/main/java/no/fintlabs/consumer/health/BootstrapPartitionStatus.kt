package no.fintlabs.consumer.health

data class BootstrapPartitionStatus(
    val partition: String,
    val endOffset: Long,
    val processedOffset: Long?,
    val caughtUp: Boolean,
)
