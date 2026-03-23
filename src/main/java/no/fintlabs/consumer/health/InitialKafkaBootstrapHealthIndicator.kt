package no.fintlabs.consumer.health

import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.stereotype.Component

@Component("initialKafkaBootstrap")
class InitialKafkaBootstrapHealthIndicator(
    private val initialKafkaBootstrapTracker: InitialKafkaBootstrapTracker,
) : HealthIndicator {
    override fun health(): Health {
        val snapshot = initialKafkaBootstrapTracker.snapshot()
        val builder = if (snapshot.ready) Health.up() else Health.outOfService()

        return builder
            .withDetail("ready", snapshot.ready)
            .withDetail("blockingListeners", snapshot.blockingListeners.size)
            .withDetail(
                "listeners",
                snapshot.blockingListeners.associate { listener ->
                    listener.listenerId to
                        mapOf(
                            "assignmentSeen" to listener.assignmentSeen,
                            "completed" to listener.completed,
                            "assignedPartitions" to listener.assignedPartitions,
                            "caughtUpPartitions" to listener.caughtUpPartitions,
                        )
                },
            ).build()
    }
}
