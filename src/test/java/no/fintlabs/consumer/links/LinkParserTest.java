package no.fintlabs.consumer.links;

import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    }

    @Test
    void testRemovePlaceholdersSuccess() {
        String relationName = "test";
        fintResource.addLink(relationName, Link.with("idField/idValue"));
        linkParser.removePlaceholders(resourceName, fintResource);
        assertEquals(fintResource.getLinks().get(relationName).getFirst().getHref(), "idField/idValue");
    }

    @Test
    void testRemovePlaceholdersSuccessWhenManyLinkSegments() {
        String relationName = "test";
        fintResource.addLink(relationName, Link.with("this/is/too/many/segments/idField/idValue"));
        linkParser.removePlaceholders(resourceName, fintResource);
        assertEquals(fintResource.getLinks().get(relationName).getFirst().getHref(), "idField/idValue");
    }

    @Test
    void testLinksContinueToGetProcessedWhenLinkExceptionIsThrown() throws LinkError {
        String relationName1 = "test1";
        String relationName2 = "test2";

        fintResource.addLink(relationName1, Link.with("this/link/throws/exception"));
        fintResource.addLink(relationName2, Link.with("another/link/structure/idField2/idValue2"));

        doThrow(new LinkError("Test Exception")).when(linkValidator).segmentsIsValid(any(String[].class), eq(fintResource.getLinks().get(relationName1).getFirst()));
        linkParser.removePlaceholders(resourceName, fintResource);

        assertEquals("this/link/throws/exception", fintResource.getLinks().get(relationName1).getFirst().getHref());
        assertEquals("idField2/idValue2", fintResource.getLinks().get(relationName2).getFirst().getHref());
    }

    @Test
    void testPublishErrorsIsCalledWhenLinkExceptionIsThrown() throws LinkError {
        String relationName1 = "test1";

        fintResource.addLink(relationName1, Link.with("this/link/throws/exception"));

        doThrow(new LinkError("Test Exception")).when(linkValidator).segmentsIsValid(any(String[].class), eq(fintResource.getLinks().get(relationName1).getFirst()));
        linkParser.removePlaceholders(resourceName, fintResource);

        verify(linkErrorProducer).publishErrors(anyString(), anyList());
    }

    @Test
    void testPublishErrorsIsCalledWhenLinksIsNull() throws LinkError {
        fintResource.setLinks(new HashMap<>());
        fintResource.getLinks().put("relationName", null);

        linkParser.removePlaceholders(resourceName, fintResource);

        verify(linkErrorProducer).publishErrors(anyString(), anyList());
    }

    @Test
    void testRelationLinksGetsNewListIfLinkListIsNull() throws LinkError {
        String relationName = "relationName";
        fintResource.setLinks(new HashMap<>());
        fintResource.getLinks().put(relationName, null);

        linkParser.removePlaceholders(resourceName, fintResource);

        assertEquals(fintResource.getLinks().get(relationName), new ArrayList<>());
        verify(linkErrorProducer).publishErrors(anyString(), anyList());
    }

}
