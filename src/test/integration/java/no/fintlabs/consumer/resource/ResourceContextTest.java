package no.fintlabs.consumer.resource;

import no.fintlabs.consumer.resource.context.ResourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("utdanning-elev")
public class ResourceContextTest {

    @Autowired
    private ResourceContext resourceContext;

    @Test
    void commonResource_ShouldBePresent() {
        assertTrue(resourceContext.getResourceNames().contains("person"));
    }

    @Test
    void commonResource_ShouldNotBePresent_WhenNotRelatedToThisComponent() {
        assertFalse(resourceContext.getResourceNames().contains("virksomhet"));
    }

    @Test
    void resourceBelongingToComponent_ShouldBePresent() {
        assertTrue(resourceContext.getResourceNames().contains("elev"));
    }

    @Test
    void resourceWithTheSameDomain_ShouldNotPresent() {
        assertFalse(resourceContext.getResourceNames().contains("elevfravar"));
    }

    @Test
    void resourceBelongingToOtherComponent_ShouldNotBePresent() {
        assertFalse(resourceContext.getResourceNames().contains("otstatus"));
    }

}
