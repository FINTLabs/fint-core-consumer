package no.fintlabs.consumer.links.unit;

import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import no.fintlabs.consumer.links.LinkParser;
import no.fintlabs.consumer.links.validator.LinkValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class LinkParserTest {

    @Mock
    private LinkErrorProducer linkErrorProducer;

    @Mock
    private LinkValidator linkValidator;

    @InjectMocks
    private LinkParser linkParser;

    private final String resourceName = "elevfravar";
    private FintResource fintResource;

    @BeforeEach
    void setup() {
        fintResource = new ElevfravarResource();
        Mockito.lenient().when(linkValidator.validLink(any(Link.class), anyList())).thenReturn(true);
        Mockito.lenient().when(linkValidator.segmentsIsValid(any(String[].class), anyList())).thenReturn(true);
        Mockito.lenient().when(linkValidator.validateIdField(anyString(), anyString(), anyString(), anyList())).thenReturn(true);
    }

    @Test
    void testRemovePlaceholdersSuccess() {
        when(linkValidator.validLink(any(Link.class), anyList())).thenReturn(true);
        when(linkValidator.segmentsIsValid(any(String[].class), anyList())).thenReturn(true);
        when(linkValidator.validateIdField(anyString(), anyString(), anyString(), anyList())).thenReturn(true);

        String relationName = "test";
        fintResource.addLink(relationName, Link.with("idField/idValue"));
        linkParser.removePlaceholders(resourceName, fintResource, new ArrayList<>());
        assertEquals(fintResource.getLinks().get(relationName).getFirst().getHref(), "idfield/idValue");
    }

    @Test
    void testRemovePlaceholdersSuccessWhenManyLinkSegments() {
        when(linkValidator.validLink(any(Link.class), anyList())).thenReturn(true);
        when(linkValidator.segmentsIsValid(any(String[].class), anyList())).thenReturn(true);
        when(linkValidator.validateIdField(anyString(), anyString(), anyString(), anyList())).thenReturn(true);

        String relationName = "test";
        fintResource.addLink(relationName, Link.with("this/is/too/many/segments/idField/idValue"));

        linkParser.removePlaceholders(resourceName, fintResource, new ArrayList<>());
        assertEquals(fintResource.getLinks().get(relationName).getFirst().getHref(), "idfield/idValue");
    }

    @Test
    void testSuccess_WhenRelationLinkIsNull() {
        String relationName = "relationName";
        fintResource.setLinks(new HashMap<>());
        fintResource.getLinks().put(relationName, null);

        ArrayList<LinkError> linkErrors = new ArrayList<>();

        linkParser.removePlaceholders(resourceName, fintResource, linkErrors);

        assertEquals(linkErrors.size(), 0);
    }

}
