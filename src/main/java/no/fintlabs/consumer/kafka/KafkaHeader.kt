package no.fintlabs.consumer.kafka;

import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;

public class KafkaHeader {

    public static long getLong(Header header) {
        return ByteBuffer.allocate(8)
                .put(header.value())
                .flip()
                .getLong();
    }

}
