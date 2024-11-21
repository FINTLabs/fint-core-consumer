package no.fintlabs.consumer.resource;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.ElevResource;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class ResourceMapperTest {

    @Autowired
    private ResourceMapper resourceMapper;

    // Mocking the kafka behaviour

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockBean
    private KafkaAdmin kafkaAdmin;

    @Test
    public void mapResourceSuccess() {
        ElevResource elevResource = new ElevResource();
        elevResource.setSystemId(new Identifikator(){{ setIdentifikatorverdi("123321"); }});

        elevResource.addElevforhold(Link.with("test/link"));
        FintResource fintResource = resourceMapper.mapResource("elev", elevResource);
        assertEquals(fintResource.getLinks().get("elevforhold").getFirst().getHref(), "test/link");
    }

}
