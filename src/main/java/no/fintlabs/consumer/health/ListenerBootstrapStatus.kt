package no.fintlabs.consumer.health

data class ListenerBootstrapStatus(
    val listenerId: String,
    val assignmentSeen: Boolean,
    val completed: Boolean,
    val assignedPartitions: Int,
    val caughtUpPartitions: Int,
    val partitions: List<BootstrapPartitionStatus>,
)
