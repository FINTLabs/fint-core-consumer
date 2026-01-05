package no.fintlabs.consumer.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "fint.consumer")
data class ConsumerConfiguration(
    val baseUrl: String,
    val orgId: String,
    val domain: String,
    val packageName: String,
    val podUrl: String,
    var autorelation: Boolean = true,
) {
    val componentUrl: String
        get() = "$baseUrl/$domain/$packageName"

    fun matchesConfiguration(
        domain: String,
        packageName: String,
        orgId: String,
    ): Boolean =
        this.domain.equals(domain, ignoreCase = true) &&
            this.packageName.equals(packageName, ignoreCase = true) &&
            this.orgId.equals(formatOrgId(orgId), ignoreCase = true)

    private fun formatOrgId(orgId: String): String = orgId.replace(Regex("[_-]"), ".")
}
