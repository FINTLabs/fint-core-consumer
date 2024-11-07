package no.fintlabs.consumer.links.unit;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.links.LinkGenerator;
import no.fintlabs.consumer.links.LinkRelations;
import no.fintlabs.reflection.ReflectionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LinkGeneratorTest {

    @Mock
    private ConsumerConfiguration configuration;

    @Mock
    private LinkRelations linkRelations;

    @Mock
    private ReflectionService reflectionService;

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
    void testSetEmptySelfLinkListIfNull() {
        assertNull(resource.getSelfLinks());
        linkGenerator.resetAndGenerateSelfLinks(resourceName, resource, new ArrayList<>());
        assertNotNull(resource.getSelfLinks());
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

    @Test
    void testGenerateSelfLinksAddsErrorToListWhenSelfLinksIsNull() {
        resource = new ElevfravarResource();
        ArrayList<LinkError> linkErrors = new ArrayList<>();
        linkGenerator.resetAndGenerateSelfLinks(resourceName, resource, linkErrors);

        assertEquals(linkErrors.size(), 1);
    }

    @Test
    void testGenerateRelationsLinksSuccess() {
        String relationName = "elevforhold";
        ArrayList<Link> elevforholdLinks = new ArrayList<>();
        elevforholdLinks.add(Link.with("systemid/123"));
        resource.getLinks().put(relationName, elevforholdLinks);

        when(configuration.getBaseUrl()).thenReturn("https://example.com");
        when(linkRelations.getRelationUri(resourceName, relationName)).thenReturn("utdanning/elev/elevforhold");
        when(reflectionService.relationNameIsNotAReference(relationName)).thenReturn(true);

        linkGenerator.generateRelationLinks(resourceName, resource);

        assertNotNull(resource.getLinks().get(relationName));
        assertEquals(resource.getLinks().get(relationName).size(), 1);
        assertEquals("https://example.com/utdanning/elev/elevforhold/systemid/123", resource.getLinks().get(relationName).getFirst().getHref());
    }

    @Test
    void testGenerateRelationsDoesntGenerateSelfLinks() {
        linkGenerator.generateRelationLinks(resourceName, resource);
        assertNull(resource.getSelfLinks());
    }

}
