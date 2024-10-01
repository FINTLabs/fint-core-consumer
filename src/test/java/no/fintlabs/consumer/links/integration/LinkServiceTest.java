package no.fintlabs.consumer.links.integration;

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
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
public class LinkServiceTest {

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
    public void testMapLinksSuccess_WithAllRelations() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource("test123", 1, 5);

        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getSelfLinks(), 1, "https://test.felleskomponent.no/utdanning/vurdering/elevfravar/systemid/test123");
        testLinks(resource.getElevforhold(), 1, "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/0");
        testLinks(resource.getFravarsregistrering(), 5, "https://test.felleskomponent.no/utdanning/vurdering/fravarsregistrering/systemid/0");

        verify(linkErrorProducer, never()).publishErrors(anyString(), anyList());
    }

    @Test
    public void testMapLinksSuccess_WithDifferentSegmentVariations() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource("test123", 0, 0);
        resource.addElevforhold(Link.with("SYSTEMID/IDVERDI123")); // index 0
        resource.addElevforhold(Link.with("/systemid/123123")); // index 1
        resource.addElevforhold(Link.with("thiadsfadufhaudfhausdfhausdfhasudf/asudhfauhdfaushdfua/systemid/123")); // index 2
        resource.addElevforhold(Link.with("https://ops.fellesJegSkrevFeil.no/utdanning/VarDetFravar?/systemid/ff33")); // index 3

        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getElevforhold(), 4, "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/idverdi123");
        assertEquals(resource.getElevforhold().get(1).getHref(), "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/123123");
        assertEquals(resource.getElevforhold().get(2).getHref(), "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/123");
        assertEquals(resource.getElevforhold().get(3).getHref(), "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/ff33");

        verify(linkErrorProducer, never()).publishErrors(anyString(), anyList());
    }

    @Test
    public void testMapLinksSuccess_WhenResettingSelfLinks() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource("test123", 1, 0);
        resource.addSelf(Link.with("https://Not.RelatedTo.AnyIdFields.InResource/systemid/321"));

        assertEquals(resource.getSelfLinks().size(), 1);
        assertEquals(resource.getSelfLinks().getFirst().getHref(), "https://Not.RelatedTo.AnyIdFields.InResource/systemid/321");
        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getSelfLinks(), 1, "https://test.felleskomponent.no/utdanning/vurdering/elevfravar/systemid/test123");
        testLinks(resource.getElevforhold(), 1, "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/0");

        verify(linkErrorProducer, never()).publishErrors(anyString(), anyList());
    }

    @Test
    public void testMapLinksPublishError_WhenRequiredRelationIsNotSet_ButContinuesGenerationOfSelfLinks() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource("test123", 0, 0); // Doesn't generate an elevforhold

        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getSelfLinks(), 1, "https://test.felleskomponent.no/utdanning/vurdering/elevfravar/systemid/test123");

        verify(linkErrorProducer, atMostOnce()).publishErrors(anyString(), anyList());
    }

    @Test
    public void testMapLinksPublishError_WhenIdValueIsEmpty_ButContinuesGenerationOfLinks() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource("test123", 1, 0); // Doesn't generate an elevforhold
        resource.addElevforhold(Link.with("idField/"));

        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getSelfLinks(), 1, "https://test.felleskomponent.no/utdanning/vurdering/elevfravar/systemid/test123");
        testLinks(resource.getElevforhold(), 2, "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/0");

        verify(linkErrorProducer, atMostOnce()).publishErrors(anyString(), anyList());
    }

    @Test
    public void testMapLinksPublishError_WhenIdFieldIsEmpty_ButContinuesGenerationOfLinks() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource("test123", 1, 0); // Doesn't generate an elevforhold
        resource.addElevforhold(Link.with("/idValue"));

        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getSelfLinks(), 1, "https://test.felleskomponent.no/utdanning/vurdering/elevfravar/systemid/test123");
        testLinks(resource.getElevforhold(), 2, "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/0");

        verify(linkErrorProducer, atMostOnce()).publishErrors(anyString(), anyList());
    }

    @Test
    public void testMapLinksPublishesError_WhenSelfLinkIsNull_ButContinuesGenerationOfLinks() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource(null, 1, 2);

        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getElevforhold(), 1, "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/0");
        testLinks(resource.getFravarsregistrering(), 2, "https://test.felleskomponent.no/utdanning/vurdering/fravarsregistrering/systemid/0");

        verify(linkErrorProducer, atMostOnce()).publishErrors(anyString(), anyList());
    }

    @Test
    public void testMapLinksPublishesError_WhenLinkHasTooFewSegments_ButContinuesGenerationOfLinks() {
        String resourceName = "elevfravar";
        ElevfravarResource resource = createValidResource("bomba123", 1, 2);
        resource.addElevforhold(Link.with("OnlyOneSegMentHereHi"));

        linkService.mapLinks(resourceName, resource);

        testLinks(resource.getSelfLinks(), 1, "https://test.felleskomponent.no/utdanning/vurdering/elevfravar/systemid/bomba123");
        testLinks(resource.getElevforhold(), 2, "https://test.felleskomponent.no/utdanning/elev/elevforhold/systemid/0");
        testLinks(resource.getFravarsregistrering(), 2, "https://test.felleskomponent.no/utdanning/vurdering/fravarsregistrering/systemid/0");

        verify(linkErrorProducer, atMostOnce()).publishErrors(anyString(), anyList());
    }

    private void testLinks(List<Link> links, int linksSize, String compareLink) {
        assertNotNull(links);
        assertEquals(links.size(), linksSize);
        assertEquals(links.getFirst().getHref(), compareLink);
    }

    private ElevfravarResource createValidResource(String systemId, int amountOfElevforholdLinks, int amountOfRegistrationLinks) {
        ElevfravarResource resource = new ElevfravarResource();
        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(systemId);
        resource.setSystemId(identifikator);

        for (int i = 0; i < amountOfElevforholdLinks; i++) {
            resource.addElevforhold(Link.with("systemid/%s".formatted(i)));
        }

        for (int i = 0; i < amountOfRegistrationLinks; i++) {
            resource.addFravarsregistrering(Link.with("systemId/%s".formatted(i)));
        }

        return resource;
    }

}
