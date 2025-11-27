package no.fintlabs.consumer.config

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@SpringBootTest(classes = [ConsumerConfigurationTest.PropertiesTestConfig::class])
@TestPropertySource(properties = [
    "fint.relation.base-url=https://testorg.no",
    "fint.consumer.domain=domain",
    //"fint.consumer.org-id=org",
    "fint.org-id=foo.org",
    "fint.consumer.package=package"
])
internal class ConsumerConfigurationTest {
    @Configuration
    @EnableConfigurationProperties(ConsumerConfiguration::class)
    open class PropertiesTestConfig

    @Autowired
    lateinit var consumerConfiguration: ConsumerConfiguration

    @Test
    fun getComponentUrl() {
        assertEquals("https://testorg.no/domain/package", consumerConfiguration.componentUrl)
    }

    @Test
    fun `matchesConfiguration is true when domain, package and orgId is correct, independent of case`() {
        assertTrue(consumerConfiguration.matchesConfiguration("domain", "package", "foo.org"))
        assertTrue(consumerConfiguration.matchesConfiguration("DOMAIN", "package", "foo.org"))
        assertTrue(consumerConfiguration.matchesConfiguration("domain", "Package", "foo.org"))
        assertTrue(consumerConfiguration.matchesConfiguration("domain", "package", "foo.ORG"))
    }

    @Test
    fun `matchesConfiguration is true when orgId contains dash or underscore instead of dot`() {
        assertTrue(consumerConfiguration.matchesConfiguration("domain", "package", "foo_org"))
        assertTrue(consumerConfiguration.matchesConfiguration("domain", "package", "foo_ORG"))
        assertTrue(consumerConfiguration.matchesConfiguration("domain", "package", "foo-ORG"))
    }

    @Test
    fun `matchesConfiguration is false when domain, package and orgId does not match`() {
        assertFalse(consumerConfiguration.matchesConfiguration("another-domain", "package", "org"))
        assertFalse(consumerConfiguration.matchesConfiguration("domain", "another-package", "org"))
        assertFalse(consumerConfiguration.matchesConfiguration("domain", "package", "another-org"))
        assertFalse(consumerConfiguration.matchesConfiguration("domain", "package", "foo..org"))
        assertFalse(consumerConfiguration.matchesConfiguration("domain", "package", "foo--org"))
    }
}