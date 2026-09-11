package no.fintlabs.consumer.links

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.JacksonConfiguration
import no.fintlabs.consumer.config.WebFluxConfig
import no.fintlabs.consumer.resource.context.ResourceContext
import no.fintlabs.consumer.resource.context.ResourceContextCache
import no.fintlabs.model.resource.FintResources
import no.fintlabs.reflection.ReflectionCache
import no.fintlabs.reflection.ReflectionInitializer
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.elev.ElevResource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.web.codec.CodecCustomizer
import org.springframework.context.annotation.Import
import org.springframework.http.codec.EncoderHttpMessageWriter
import org.springframework.http.codec.ServerCodecConfigurer
import org.springframework.http.codec.json.Jackson2JsonEncoder
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig

@SpringJUnitConfig(classes = [LinkSerializerTest.Config::class])
@TestPropertySource(
    properties = [
        "fint.consumer.base-url=https://test.felleskomponent.no",
        "fint.consumer.domain=utdanning",
        "fint.consumer.package-name=elev",
        "fint.consumer.org-id=fintlabs.no",
        "fint.consumer.writeable=klasse",
        "fint.consumer.pod-url=http://test",
    ],
)
class LinkSerializerTest {
    @EnableConfigurationProperties(ConsumerConfiguration::class)
    @Import(
        ReflectionCache::class,
        ReflectionInitializer::class,
        ResourceContextCache::class,
        ResourceContext::class,
        JacksonConfiguration::class,
        WebFluxConfig::class,
    )
    class Config

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    @Qualifier("webObjectMapper")
    private lateinit var webObjectMapper: ObjectMapper

    @Autowired
    private lateinit var linkEncodingCodecCustomizer: CodecCustomizer

    private val componentUrl = "https://test.felleskomponent.no/utdanning/elev"

    @Test
    fun `web mapper encodes idValue in hrefs`() {
        val resource = createResource("AB 12")

        val json = webObjectMapper.writeValueAsString(resource)

        assertTrue(json.contains("$componentUrl/elev/systemid/AB%2012"))
        assertTrue(json.contains("$componentUrl/person/fodselsnummer/24051764852"))
        assertFalse(json.contains("systemid/AB 12"))
    }

    @Test
    fun `serialization does not mutate the resource`() {
        val resource = createResource("AB 12")

        webObjectMapper.writeValueAsString(resource)

        assertEquals("$componentUrl/elev/systemid/AB 12", resource.selfLinks.first().href)
        assertEquals("AB 12", resource.systemId.identifikatorverdi)
    }

    @Test
    fun `repeated serialization does not double-encode`() {
        val resource = createResource("50%12")

        val first = webObjectMapper.writeValueAsString(resource)
        val second = webObjectMapper.writeValueAsString(resource)

        assertEquals(first, second)
        assertTrue(first.contains("$componentUrl/elev/systemid/50%2512"))
    }

    @Test
    fun `primary mapper keeps hrefs decoded`() {
        val resource = createResource("AB 12")

        val json = objectMapper.writeValueAsString(resource)

        assertTrue(json.contains("$componentUrl/elev/systemid/AB 12"))
        assertFalse(json.contains("AB%2012"))
    }

    @Test
    fun `pagination links pass through while entity links are encoded`() {
        val resources = FintResources(listOf(createResource("AB 12")))
        resources.addSelf(Link.with("$componentUrl/elev?offset=0&size=10"))

        val json = webObjectMapper.writeValueAsString(resources)

        assertTrue(json.contains("$componentUrl/elev?offset=0&size=10"))
        assertTrue(json.contains("$componentUrl/elev/systemid/AB%2012"))
    }

    @Test
    fun `codec customizer registers web mapper as json encoder`() {
        val configurer = ServerCodecConfigurer.create()

        linkEncodingCodecCustomizer.customize(configurer)

        val encoder =
            configurer.writers
                .filterIsInstance<EncoderHttpMessageWriter<*>>()
                .map { it.encoder }
                .filterIsInstance<Jackson2JsonEncoder>()
                .first()
        assertSame(webObjectMapper, encoder.objectMapper)
    }

    private fun createResource(id: String): ElevResource =
        ElevResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
            addSelf(Link.with("$componentUrl/elev/systemid/$id"))
            addPerson(Link.with("$componentUrl/person/fodselsnummer/24051764852"))
        }
}
