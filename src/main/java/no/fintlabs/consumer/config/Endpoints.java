package no.fintlabs.consumer.config;

public class Endpoints {


    public static final String DEFAULT_ENDPOINT = "${fint.consumer.domain}/${fint.consumer.package}";
    public static final String RESOURCE_ENDPOINT = DEFAULT_ENDPOINT + "/{resource}";
    public static final String ADMIN = DEFAULT_ENDPOINT + "/admin";

    public static final String COMPONENT_BASE_URL = "https://api.felleskomponent.no/utdanning/vurdering"; // Test

    public static final String LAST_UPDATED = "/last-updated";
    public static final String CACHE_SIZE = "/cache/size";
    public static final String BY_ID = "/{idField}/{idValue}";
    public static final String STATUS_ID = "/status/{id}";


}
