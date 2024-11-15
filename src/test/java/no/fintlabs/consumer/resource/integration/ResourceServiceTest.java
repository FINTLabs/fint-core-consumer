package no.fintlabs.consumer.resource.integration;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.resource.ResourceService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class ResourceServiceTest {

    @Autowired
    private ResourceService resourceService;

    @Test
    public void mapResourceAndLinksSuccess() {
        ElevfravarResource elevFravarResource = createElevFravarResource("123");
        elevFravarResource.addElevforhold(Link.with("systemid/321"));

        FintResource fintResource = resourceService.mapResourceAndLinks("elevfravar", elevFravarResource);

        assertEquals(
                "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/321",
                fintResource.getLinks().get("elevforhold").getFirst().getHref()
        );
    }

    private ElevfravarResource createElevFravarResource(String id) {
        ElevfravarResource elevfravarResource = new ElevfravarResource();
        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(id);
        elevfravarResource.setSystemId(identifikator);
        return elevfravarResource;
    }

}
