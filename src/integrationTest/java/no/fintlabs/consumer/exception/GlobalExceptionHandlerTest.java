package no.fintlabs.consumer.exception;

import no.fintlabs.consumer.exception.kafka.ConsumerErrorPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;


@SpringBootTest
@ActiveProfiles("utdanning-vurdering")
@EmbeddedKafka
@AutoConfigureWebTestClient
public class GlobalExceptionHandlerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoSpyBean
    private ConsumerErrorPublisher consumerErrorPublisher;


    @BeforeEach
    void setUp() {
    }

    @Test
    @DisplayName("Should return 404 when id is not found")
    void shouldReturn404OnNotFoundId() {
        webTestClient.get()
                .uri("/utdanning/vurdering/elevfravar/1234")
                .exchange()
                .expectStatus()
                .is4xxClientError();

        verifyNoInteractions(consumerErrorPublisher);
    }

    @Test
    @DisplayName("Should return 404 when resource is not found")
    void shouldReturn404WhenResourceIsNotFound() {
        webTestClient.get()
                .uri("/utdanning/vurdering/elevfravar/systemid/1234")
                .exchange()
                .expectStatus()
                .is4xxClientError();

        verifyNoInteractions(consumerErrorPublisher);
    }

    @Test
    @DisplayName("Should return 500 when we get a RuntimeException")
    void shouldReturn500WhenWeGetARuntimeException() {
        webTestClient.get()
                .uri("/bob")
                .exchange()
                .expectStatus()
                .is5xxServerError();

        verify(consumerErrorPublisher).publish(any());
    }

    @RestController
    static class TestController {
        @GetMapping("/bob")
        public Mono<String> triggerError() {
            return Mono.error(new RuntimeException("Something went wrong!"));
        }
    }
}