package no.fintlabs.consumer.links.integrasion;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import no.fintlabs.consumer.links.LinkService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SpringBootTest
public class LinkServiceIntegrasionTest {

    @Autowired
    private LinkService linkService;

    @Autowired
    private ConsumerConfiguration configuration;

    // Mocking the kafka behaviour

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockBean
    private KafkaAdmin kafkaAdmin;

    @MockBean
    private LinkErrorProducer linkErrorProducer;

    @Test
    public void testMapLinksSuccess() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource("test123", "321", 5);

        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getSelfLinks(), 1, "https://test.felleskomponent.no/utdanning/vurdering/elevfravar/systemid/test123");
        testLinks(resource.getElevforhold(), 1, "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/321");
        testLinks(resource.getFravarsregistrering(), 5, "https://test.felleskomponent.no/utdanning/vurdering/fravarsregistrering/systemid/0");

        verify(linkErrorProducer, never()).publishErrors(anyString(), anyList());
    }

    private void testLinks(List<Link> links, int linksSize, String compareLink) {
        assertNotNull(links);
        assertEquals(links.size(), linksSize);
        assertEquals(links.getFirst().getHref(), compareLink);
    }

    private ElevfravarResource createValidResource(String systemId, String elevforholdId, int amountOfRegistrationLinks) {
        ElevfravarResource resource = new ElevfravarResource();
        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(systemId);
        resource.setSystemId(identifikator);
        resource.addElevforhold(Link.with("systemid/%s".formatted(elevforholdId)));

        for (int i = 0; i < amountOfRegistrationLinks; i++) {
            resource.addFravarsregistrering(Link.with("systemId/%s".formatted(i)));
        }

        return resource;
    }

}
