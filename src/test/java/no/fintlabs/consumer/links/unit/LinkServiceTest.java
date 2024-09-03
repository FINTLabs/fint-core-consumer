package no.fintlabs.consumer.links.unit;

import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import no.fintlabs.consumer.links.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class LinkServiceTest {


    @Mock
    private LinkParser linkParser;

    @Mock
    private LinkPaginator linkPaginator;

    @Mock
    private LinkGenerator linkGenerator;

    @Mock
    private LinkErrorProducer linkErrorProducer;

    @Mock
    private LinkValidator linkValidator;

    @InjectMocks
    private LinkService linkService;
    private final String resourceName = "elevfravar";
    private FintResource resource;

    @BeforeEach
    void setup() {
        resource = new ElevfravarResource();
    }

    @Test
    void shouldPublishErrorWhenErrorsPresent() {
        ArrayList<LinkError> linkErrors = new ArrayList<>();

        doAnswer(invocation -> {
            ((ArrayList<LinkError>) invocation.getArgument(2)).add(new LinkError("Test error"));
            return null;
        }).when(linkGenerator).resetAndGenerateSelfLinks(eq(resourceName), eq(resource), anyList());

        linkService.mapLinks(resourceName, resource);

        verify(linkErrorProducer, times(1)).publishErrors(anyString(), anyList());
    }

    @Test
    void shouldNotPublishErrorWhenNoErrors() {
        linkService.mapLinks(resourceName, resource);

        verify(linkErrorProducer, never()).publishErrors(anyString(), anyList());
    }

}
