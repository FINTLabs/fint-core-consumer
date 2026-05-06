package no.fintlabs.consumer.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "fint.consumer.cache")
data class FintCacheProperties(
    val basePath: String = "${System.getProperty("java.io.tmpdir")}/fint-cache",
    val blockCacheSizeMb: Long = 8L,
)
