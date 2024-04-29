package no.fintlabs.consumer.defaultendpoint;

import lombok.Getter;
import no.fintlabs.consumer.defaultendpoint.EndpointDetails;

import java.util.HashMap;
import java.util.Map;

@Getter
public class DefaultEndpoints {

    private final Map<String, EndpointDetails> defaultEndpointDetails = new HashMap<>();

    public void addEndpointDetails(String resource, EndpointDetails endpointDetails) {
        this.defaultEndpointDetails.put(resource, endpointDetails);
    }

}
