package no.fintlabs.consumer.resource;

import no.fintlabs.consumer.exception.resource.IdentificatorNotFoundException;
import no.fintlabs.consumer.exception.resource.ResourceNotFoundException;
import no.fintlabs.consumer.exception.resource.ResourceNotWriteableException;
import no.fintlabs.consumer.resource.aspect.IdentifierAspect;
import no.fintlabs.consumer.resource.aspect.ResourceAspect;
import no.fintlabs.consumer.resource.aspect.WriteableAspect;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@ActiveProfiles("utdanning-elev")
public class ResourceAspectTest {

    @Autowired
    private IdentifierAspect identifierAspect;

    @Autowired
    private ResourceAspect resourceAspect;

    @Autowired
    private WriteableAspect writeableAspect;

    private final String validResource = "elev";
    private final String nonWriteableResource = "elevforhold";

    @Test
    void testResourceSuccess() {
        resourceAspect.checkResource(nonWriteableResource);
    }

    @Test
    void testResourceFailure_WhenResourceDoesNotExistInComponent() {
        assertThrows(ResourceNotFoundException.class, () -> resourceAspect.checkResource("asdfasdfasdf"));
    }

    @Test
    void testIdentifierSuccess() {
        identifierAspect.checkIdField(nonWriteableResource, "systemid");
    }

    @Test
    void testIdentifierFailure_WhenIdFieldDoesNotMatchResource() {
        assertThrows(IdentificatorNotFoundException.class, () -> identifierAspect.checkIdField(nonWriteableResource, "fodselsnummer"));
    }

    @Test
    void testWriteableAspectSuccess() {
        writeableAspect.checkWriteable(validResource);
    }

    @Test
    void testWriteableAspectFailure_WhenResourceIsNotWriteable() {
        assertThrows(ResourceNotWriteableException.class, () -> writeableAspect.checkWriteable(nonWriteableResource));
    }

    @Test
    void testWriteableAspectSuccess_WhenResourceIsInWriteableConfig() {
        writeableAspect.checkWriteable("basisgruppe");
    }

}
