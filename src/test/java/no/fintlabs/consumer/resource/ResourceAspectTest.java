package no.fintlabs.consumer.resource;

import no.fintlabs.consumer.exception.IdentificatorNotFoundException;
import no.fintlabs.consumer.exception.ResourceNotFoundException;
import no.fintlabs.consumer.exception.ResourceNotWriteableException;
import no.fintlabs.consumer.resource.aspect.IdentifierAspect;
import no.fintlabs.consumer.resource.aspect.ResourceAspect;
import no.fintlabs.consumer.resource.aspect.WriteableAspect;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
public class ResourceAspectTest {

    @Autowired
    private IdentifierAspect identifierAspect;

    @Autowired
    private ResourceAspect resourceAspect;

    @Autowired
    private WriteableAspect writeableAspect;

    // Mocking the kafka behaviour

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockBean
    private KafkaAdmin kafkaAdmin;

    private final String validResource = "elevfravar";
    private final String validWriteableResource = "eksamensgruppe";

    @Test
    void testResourceSuccess() {
        resourceAspect.checkResource(validResource);
    }

    @Test
    void testResourceFailure_WhenResourceDoesNotExistInComponent() {
        assertThrows(ResourceNotFoundException.class, () -> resourceAspect.checkResource("asdfasdfasdf"));
    }

    @Test
    void testIdentifierSuccess() {
        identifierAspect.checkIdField(validResource, "systemid");
    }

    @Test
    void testIdentifierFailure_WhenIdFieldDoesNotMatchResource() {
        assertThrows(IdentificatorNotFoundException.class, () -> identifierAspect.checkIdField(validResource, "fodselsnummer"));
    }

    @Test
    void testWriteableAspectSuccess() {
        writeableAspect.checkWriteable(validWriteableResource);
    }

    @Test
    void testWriteableAspectFailure_WhenResourceIsNotWriteable() {
        assertThrows(ResourceNotWriteableException.class, () -> writeableAspect.checkWriteable(validResource));
    }

}
