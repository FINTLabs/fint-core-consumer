package no.fintlabs.consumer.readiness;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fintlabs.consumer.offset.OffsetService;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReadinessService {

    private final RestClient consumerRestClient;
    private final OffsetService offsetService;
    private final ConsumerConfiguration configuration;

    private Map<String, Long> getExistingOffsets() {
        log.info("Fetching existing offsets from: {}/{}/{}/offset", configuration.getPodUrl(), configuration.getDomain(), configuration.getPackageName());
        return consumerRestClient.get()
                .uri("/%s/%s/offset".formatted(configuration.getDomain(), configuration.getPackageName()))
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
    }

    public boolean isReady() {
        Map<String, Long> existingOffsets = getExistingOffsets();
        Map<String, Long> offsetMap = offsetService.getOffsetMap();

        if (existingOffsets.size() != offsetMap.size()) {
            throw new IllegalArgumentException("The sizes of the maps are different!");
        }

        for (Map.Entry<String, Long> entry : existingOffsets.entrySet()) {
            String key = entry.getKey();
            Long existingValue = entry.getValue();

            if (!offsetMap.containsKey(key)) {
                throw new IllegalArgumentException("Key mismatch between maps: " + key);
            }

            Long offsetValue = offsetMap.get(key);

            if (offsetValue < existingValue) {
                return false;
            }
        }

        return true;
    }

}
