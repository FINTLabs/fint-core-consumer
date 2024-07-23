package no.temp;

import org.springframework.http.HttpMethod;

public enum SyncType {
    FULL(HttpMethod.POST),
    DELTA(HttpMethod.PATCH),
    DELETE(HttpMethod.DELETE);

    private final HttpMethod httpMethod;

    SyncType(HttpMethod httpMethod) {
        this.httpMethod = httpMethod;
    }

    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

}
