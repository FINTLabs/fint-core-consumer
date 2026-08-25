package no.fintlabs.consumer.links

import org.springframework.web.util.UriUtils
import java.nio.charset.StandardCharsets

class LinkIdValueEncoder(
    private val baseUrl: String,
    private val idFieldNames: Set<String>,
) {
    fun encode(href: String): String {
        if (!href.startsWith(baseUrl)) return href

        val idValueStart = href.lastIndexOf('/') + 1
        if (idValueStart <= baseUrl.length || idValueStart >= href.length) return href

        val idFieldStart = href.lastIndexOf('/', idValueStart - 2) + 1
        val idField = href.substring(idFieldStart, idValueStart - 1)
        if (idField.lowercase() !in idFieldNames) return href

        return href.substring(0, idValueStart) +
            UriUtils.encodePathSegment(href.substring(idValueStart), StandardCharsets.UTF_8)
    }
}
