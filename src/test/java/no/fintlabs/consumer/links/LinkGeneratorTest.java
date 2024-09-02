package no.fintlabs.consumer.links;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.LinkError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LinkGeneratorTest {

    @Mock
    private ConsumerConfiguration configuration;

    @Mock
    private LinkRelations linkRelations;

    @InjectMocks
    private LinkGenerator linkGenerator;
    private final String resourceName = "elevfravar";
    private FintResource resource;

    @BeforeEach
    void setup() {
        ElevfravarResource elevfravarResource = new ElevfravarResource();
        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi("123");
        elevfravarResource.setSystemId(identifikator);
        resource = elevfravarResource;

    }

    @Test
    void testGenerateSelfLinksSuccess() {
        when(configuration.getComponentUrl()).thenReturn("https://example.com/utdanning/vurdering");
        assertNull(resource.getSelfLinks());

        ArrayList<LinkError> linkErrors = new ArrayList<>();
        linkGenerator.resetAndGenerateSelfLinks(resourceName, resource, linkErrors);

        assertEquals(linkErrors.size(), 0);
        assertNotEquals(resource.getSelfLinks(), null);
        assertEquals(resource.getSelfLinks().size(), 1);
        assertEquals(resource.getSelfLinks().getFirst().getHref(), "https://example.com/utdanning/vurdering/elevfravar/systemid/123");
    }

    @Test
    void testResettingOfExistingSelfLinks() {
        when(configuration.getComponentUrl()).thenReturn("https://example.com/utdanning/vurdering");

        ArrayList<Link> selfLinks = new ArrayList<>();
        selfLinks.add(Link.with("I exist"));
        resource.getLinks().put("self", selfLinks);

        assertEquals(resource.getSelfLinks().size(), 1);
        assertEquals(resource.getSelfLinks().getFirst().getHref(), "I exist");

        ArrayList<LinkError> linkErrors = new ArrayList<>();
        linkGenerator.resetAndGenerateSelfLinks(resourceName, resource, linkErrors);

        assertEquals(linkErrors.size(), 0);
        assertNotEquals(resource.getSelfLinks(), null);
        assertEquals(resource.getSelfLinks().size(), 1);
        assertEquals(resource.getSelfLinks().getFirst().getHref(), "https://example.com/utdanning/vurdering/elevfravar/systemid/123");
    }


}
