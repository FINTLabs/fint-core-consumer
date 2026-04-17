package no.fintlabs.config

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * Shared base class for integration tests that need a real Kafka broker via Testcontainers.
 *
 * The broker is started once per JVM (static init on the companion object) and reused across
 * every subclass, so spinning up new tests stays cheap. Each subclass is responsible for
 * pre-creating the topics it needs — production Kafka does not auto-create topics, so tests
 * that want to mirror production must create them explicitly before the Spring context boots.
 *
 * The broker is configured for aggressive log compaction so that compaction-dependent tests
 * can observe cleaned segments within a few seconds.
 *
 * The recommended pattern is to call [createComponentTopics] from the subclass's own
 * companion-object init block, which runs after this class's companion init but before
 * Spring starts wiring any consumers:
 *
 * ```
 * class MyKafkaIT : KafkaTestcontainersSupport() {
 *     companion object { init { createComponentTopics("utdanning", "vurdering") } }
 * }
 * ```
 */
abstract class KafkaTestcontainersSupport {
    companion object {
        @JvmStatic
        val KAFKA: KafkaContainer =
            KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"))
                // Aggressive log cleaner so compaction-dependent tests don't wait minutes.
                .withEnv("KAFKA_LOG_CLEANER_ENABLE", "true")
                .withEnv("KAFKA_LOG_CLEANER_MIN_CLEANABLE_RATIO", "0.01")
                .withEnv("KAFKA_LOG_CLEANER_BACKOFF_MS", "200")
                .withEnv("KAFKA_LOG_CLEANER_CHECK_INTERVAL_MS", "200")
                .also { it.start() }

        @JvmStatic
        @DynamicPropertySource
        fun registerKafkaProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { KAFKA.bootstrapServers }
        }

        /**
         * Creates the four topics a consumer component expects at startup:
         *  - `{orgId}.{domainContext}.entity.{domain}-{package}`
         *  - `{orgId}.{domainContext}.entity.{domain}-{package}-relation-update`
         *  - `{orgId}.{domainContext}.event.{domain}-{package}-request`
         *  - `{orgId}.{domainContext}.event.{domain}-{package}-response`
         *
         * [orgIdSegment] is the topic-segment form (dots replaced with dashes), e.g. `fintlabs-no`.
         */
        fun createComponentTopics(
            domain: String,
            packageName: String,
            orgIdSegment: String = "fintlabs-no",
            domainContext: String = "fint-core",
            partitions: Int = 1,
            configs: Map<String, String> = emptyMap(),
        ) {
            val component = "$domain-$packageName"
            val names =
                listOf(
                    "$orgIdSegment.$domainContext.entity.$component",
                    "$orgIdSegment.$domainContext.entity.$component-relation-update",
                    "$orgIdSegment.$domainContext.event.$component-request",
                    "$orgIdSegment.$domainContext.event.$component-response",
                )
            createTopics(names, partitions, configs)
        }

        fun createTopics(
            names: List<String>,
            partitions: Int = 1,
            configs: Map<String, String> = emptyMap(),
        ) {
            val newTopics =
                names.map { name ->
                    NewTopic(name, partitions, 1.toShort()).configs(configs)
                }
            adminClient().use { admin -> admin.createTopics(newTopics).all().get() }
        }

        /**
         * Creates a log-compacted topic. Segment and compaction settings are tuned so that
         * records become eligible for cleaning within roughly one second after a segment rolls.
         * Callers still need to emit enough records (or enough bytes) to roll the active segment,
         * since Kafka never compacts the active segment.
         */
        fun createCompactedTopic(
            name: String,
            partitions: Int = 1,
            additionalConfigs: Map<String, String> = emptyMap(),
        ) {
            val configs =
                mapOf(
                    "cleanup.policy" to "compact",
                    "min.cleanable.dirty.ratio" to "0.01",
                    "segment.ms" to "200",
                    "segment.bytes" to "1024",
                    "min.compaction.lag.ms" to "0",
                    "max.compaction.lag.ms" to "500",
                    "delete.retention.ms" to "500",
                ) + additionalConfigs
            createTopics(listOf(name), partitions, configs)
        }

        fun adminClient(): AdminClient =
            AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KAFKA.bootstrapServers))
    }
}
