package no.fintlabs.consumer.resource

import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.elev.ElevResource
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("utdanning-elev")
@EmbeddedKafka
class ResourceServiceTest {
    @Autowired
    private lateinit var resourceService: ResourceService

    @Test
    fun mapResourceAndLinksSuccess() {
        val elevResource: ElevResource = createElevResource("123")
        elevResource.addElevforhold(Link.with("systemid/321"))

        val fintResource: FintResource = resourceService.mapResourceAndLinks("elev", elevResource as Any)

        Assertions.assertEquals(
            "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/321",
            fintResource
                .getLinks()["elevforhold"]!!
                .first()
                .href,
        )
    }

    private fun createElevResource(id: String?): ElevResource =
        ElevResource().apply {
            systemId =
                Identifikator().apply {
                    identifikatorverdi = id
                }
        }
}
