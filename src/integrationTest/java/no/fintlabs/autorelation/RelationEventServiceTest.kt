package no.fintlabs.autorelation

import no.fintlabs.autorelation.cache.RelationRuleRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import kotlin.test.assertTrue

@SpringBootTest
@EmbeddedKafka
@ActiveProfiles("utdanning-vurdering")
class RelationEventServiceTest {
    @Autowired
    private lateinit var relationRuleRegistry: RelationRuleRegistry

    @Test
    fun `fravarsregistrering is a managed resource with at least one rule`() {
        val rules = relationRuleRegistry.getRules("utdanning", "vurdering", "fravarsregistrering")

        assertTrue(rules.isNotEmpty(), "Expected fravarsregistrering to have at least one relation rule")
    }
}
