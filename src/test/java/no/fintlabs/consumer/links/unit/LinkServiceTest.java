package no.fintlabs.consumer.links.unit;

import no.fint.model.resource.FintResource;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import no.fintlabs.consumer.links.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
    void shouldNotPublishErrorWhenNoErrors() {
        linkService.mapLinks(resourceName, resource);

        verify(linkErrorProducer, never()).publishErrors(anyString(), anyList());
    }

}
