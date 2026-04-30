package no.fintlabs.consumer.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "fint.consumer.cache")
data class FintCacheProperties(
    val basePath: String = "${System.getProperty("java.io.tmpdir")}/fint-cache",
    // Shared across all FintCache instances — one LRUCache for the whole service
    val blockCacheSizeMb: Long = 64L,
    // RocksDB write buffer (memtable) per column family — default is 64 MB, far too large
    val writeBufferMb: Long = 4L,
    // Cap on total write buffer memory across all column families in one DB instance
    val totalWriteBufferMb: Long = 32L,
)
