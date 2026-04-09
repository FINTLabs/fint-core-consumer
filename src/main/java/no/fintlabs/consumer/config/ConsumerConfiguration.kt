package no.fintlabs.consumer.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.Name
import java.time.Duration

// TODO: Split up the configuration to be more modular, there's too many unrelated configurations in one location
@ConfigurationProperties(prefix = "fint.consumer")
data class ConsumerConfiguration(
    val baseUrl: String,
    @param:Name("org-id")
    private val orgIdValue: String,
    val domain: String,
    val packageName: String,
    val podUrl: String,
    val autorelation: AutorelationConfig = AutorelationConfig(),
    val coreVersionHeader: String = "2",
    val kafka: KafkaConfiguration = KafkaConfiguration(),
) {
    init {
        require(baseUrl == baseUrl.lowercase()) { "baseUrl must be lowercase: $baseUrl" }
    }

    val orgId: OrgId
        get() = OrgId.from(orgIdValue)

    val componentUrl: String
        get() = "$baseUrl/$domain/$packageName"

    fun matchesComponent(
        domainName: String,
        packageName: String,
    ): Boolean =
        this.domain.equals(domainName, ignoreCase = true) &&
            this.packageName.equals(packageName, ignoreCase = true)

    fun matchesConfiguration(
        domainName: String,
        packageName: String,
        orgId: String,
    ): Boolean =
        matchesComponent(domainName, packageName) &&
            this.orgId.matches(orgId)
}

// TODO: Cleanup configuration
data class KafkaConfiguration(
    // Entity consumption in EntityConsumer & AutoRelationEntityConsumer
    val consumeLegacyResourceTopics: Boolean = false,
    val entityConcurrency: Int = 1,
    val relationEntitySeekToBeginning: Boolean = false,
    val fetchMinBytes: Int = 65536,
    val fetchMaxWaitMs: Int = 500,
    // RelationUpdate
    val relationConcurrency: Int = 1,
    val relationPartitions: Int = 1,
    val relationRetentionTime: Duration = Duration.ofDays(7),
    // RequestFintEvent
    val requestConcurrency: Int = 1,
    val requestPartitions: Int = 1,
    val requestRetentionTime: Duration = Duration.ofDays(7),
    // ResponseFintEvent
    val responseConcurrency: Int = 1,
    // Topic management
    val ensureTopics: Boolean = true,
)

data class AutorelationConfig(
    val enabled: Boolean = true,
    val buffer: BufferConfig = BufferConfig(),
) {
    data class BufferConfig(
        /** Duration to retain unresolved relation links before eviction. Default: 7 days. */
        val ttl: Duration = Duration.ofDays(7),
    )
}
