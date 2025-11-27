package no.fintlabs.consumer.config

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@SpringBootTest(classes = [ConsumerConfigurationTest.PropertiesTestConfig::class])
@TestPropertySource(
    properties = [
        "fint.relation.base-url=https://testorg.no",
        "fint.consumer.domain=domain",
        // "fint.consumer.org-id=org",
        "fint.org-id=foo.org",
        "fint.consumer.package=package",
    ],
)
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

    @ParameterizedTest
    @CsvSource(
        "domain, package, foo.org",
        "domAin, packagE, fOO.org",
        "dOmain, PAckage, foo.OrG",
    )
    fun `matchesConfiguration is true when domain, package and orgId is correct, independent of case`(
        domain: String,
        pkg: String,
        orgId: String,
    ) = assertTrue(consumerConfiguration.matchesConfiguration(domain, pkg, orgId))

    @ParameterizedTest
    @CsvSource(
        "domain, package, foo_org",
        "domain, package, foo_ORG",
        "domain, package, foo-ORG",
    )
    fun `matchesConfiguration is true when orgId contains dash or underscore instead of dot`(
        domain: String,
        pkg: String,
        orgId: String,
    ) = assertTrue(consumerConfiguration.matchesConfiguration(domain, pkg, orgId))

    @ParameterizedTest
    @CsvSource(
        "another-domain, package, org",
        "domain, another-package, org",
        "domain, package, another-org",
        "domain, package, foo..org",
        "domain, package, foo--org",
    )
    fun `matchesConfiguration is false when domain, package and orgId does not match`(
        domain: String,
        pkg: String,
        orgId: String,
    ) = assertFalse(consumerConfiguration.matchesConfiguration(domain, pkg, orgId))
}
