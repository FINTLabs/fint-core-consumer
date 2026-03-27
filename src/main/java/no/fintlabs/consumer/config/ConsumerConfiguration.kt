package no.fintlabs.consumer.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.Name
import java.time.Duration

@ConfigurationProperties(prefix = "fint.consumer")
data class ConsumerConfiguration(
    val baseUrl: String,
    @param:Name("org-id")
    private val orgIdValue: String,
    val domain: String,
    val packageName: String,
    val podUrl: String,
    var autorelation: Boolean = true,
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
)
