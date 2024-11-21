package no.fintlabs.consumer.resource;

import no.fintlabs.consumer.kafka.LinkErrorProducer;
import no.fintlabs.reflection.ResourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class ResourceContextTest {

    @Autowired
    private ResourceContext resourceContext;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockBean
    private KafkaAdmin kafkaAdmin;

    @MockBean
    private LinkErrorProducer linkErrorProducer;

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
