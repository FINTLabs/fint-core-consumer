package no.fintlabs.consumer.links

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class LinkIdValueEncoderTest {
    private val baseUrl = "https://test.felleskomponent.no"
    private val elevUrl = "$baseUrl/utdanning/elev/elev"
    private val encoder = LinkIdValueEncoder(baseUrl, setOf("systemid", "fodselsnummer"))

    @Test
    fun `plain id is unchanged`() {
        assertEquals("$elevUrl/systemid/123", encoder.encode("$elevUrl/systemid/123"))
    }

    @Test
    fun `space is percent-encoded`() {
        assertEquals("$elevUrl/systemid/AB%2012", encoder.encode("$elevUrl/systemid/AB 12"))
    }

    @Test
    fun `hash is percent-encoded`() {
        assertEquals("$elevUrl/systemid/AB%2312", encoder.encode("$elevUrl/systemid/AB#12"))
    }

    @Test
    fun `question mark is percent-encoded`() {
        assertEquals("$elevUrl/systemid/AB%3F12", encoder.encode("$elevUrl/systemid/AB?12"))
    }

    @Test
    fun `literal percent is percent-encoded`() {
        assertEquals("$elevUrl/systemid/50%2512", encoder.encode("$elevUrl/systemid/50%12"))
    }

    @Test
    fun `norwegian characters are percent-encoded`() {
        assertEquals(
            "$elevUrl/systemid/bl%C3%A5b%C3%A6rsyltet%C3%B8y",
            encoder.encode("$elevUrl/systemid/blåbærsyltetøy"),
        )
    }

    @Test
    fun `idField segment is matched case-insensitively`() {
        assertEquals("$elevUrl/SYSTEMID/AB%2012", encoder.encode("$elevUrl/SYSTEMID/AB 12"))
    }

    @Test
    fun `allowed path characters are kept as-is`() {
        assertEquals("$elevUrl/systemid/a+b:c@d,e", encoder.encode("$elevUrl/systemid/a+b:c@d,e"))
    }

    @Test
    fun `pagination href is unchanged`() {
        val href = "$baseUrl/utdanning/elev/elev?offset=0&size=10"
        assertEquals(href, encoder.encode(href))
    }

    @Test
    fun `status href is unchanged`() {
        val href = "$baseUrl/utdanning/elev/elev/status/abc-123"
        assertEquals(href, encoder.encode(href))
    }

    @Test
    fun `href outside baseUrl is unchanged`() {
        val href = "https://external.example.com/whatever/systemid/AB 12"
        assertEquals(href, encoder.encode(href))
    }

    @Test
    fun `id containing slash is unchanged`() {
        val href = "$elevUrl/systemid/AB/12"
        assertEquals(href, encoder.encode(href))
    }

    @Test
    fun `href with trailing slash is unchanged`() {
        val href = "$elevUrl/systemid/"
        assertEquals(href, encoder.encode(href))
    }

    @Test
    fun `href equal to baseUrl is unchanged`() {
        assertEquals(baseUrl, encoder.encode(baseUrl))
    }
}
