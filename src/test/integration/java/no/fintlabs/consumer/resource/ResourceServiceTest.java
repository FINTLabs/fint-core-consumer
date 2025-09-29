package no.fintlabs.consumer.resource;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.ElevResource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("utdanning-elev")
public class ResourceServiceTest {

    @Autowired
    private ResourceService resourceService;

    @Test
    public void mapResourceAndLinksSuccess() {
        ElevResource elevResource = createElevResource("123");
        elevResource.addElevforhold(Link.with("systemid/321"));

        FintResource fintResource = resourceService.mapResourceAndLinks("elev", elevResource);

        assertEquals(
                "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/321",
                fintResource.getLinks().get("elevforhold").getFirst().getHref()
        );
    }

    private ElevResource createElevResource(String id) {
        return new ElevResource() {{
            setSystemId(new Identifikator() {{
                setIdentifikatorverdi(id);
            }});
        }};
    }

}
