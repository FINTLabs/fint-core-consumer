package no.fintlabs.consumer.resource;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.ElevResource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("utdanning-elev")
@EmbeddedKafka
public class ResourceMapperServiceTest {

    @Autowired
    private ResourceMapperService resourceMapper;

    @Test
    public void mapResourceSuccess() {
        ElevResource elevResource = new ElevResource();
        elevResource.setSystemId(new Identifikator() {{
            setIdentifikatorverdi("123321");
        }});

        elevResource.addElevforhold(Link.with("test/link"));
        FintResource fintResource = resourceMapper.mapResource("elev", elevResource);
        assertEquals(fintResource.getLinks().get("elevforhold").getFirst().getHref(), "test/link");
    }

}
