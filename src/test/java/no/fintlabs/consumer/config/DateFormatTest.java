package no.fintlabs.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
public class DateFormatTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void testDateSerializationToISO8601() throws Exception {
        Instant date = Instant.parse("2023-01-01T00:00:00Z");

        String serializedDate = objectMapper.writeValueAsString(date);

        assertEquals("\"2023-01-01T00:00:00Z\"", serializedDate);
    }

    @Test
    public void testDateDeserializationFromISO8601() throws Exception {
        String isoDate = "\"2023-01-01T00:00:00Z\"";

        Instant deserializedDate = objectMapper.readValue(isoDate, Instant.class);

        Instant expectedDate = Instant.parse("2023-01-01T00:00:00Z");
        assertEquals(expectedDate, deserializedDate);
    }

    @Test
    public void testInvalidDateFormatThrowsError() {
        String invalidDate = "\"01-01-2023\"";

        assertThrows(Exception.class, () -> objectMapper.readValue(invalidDate, Instant.class));
    }

}
