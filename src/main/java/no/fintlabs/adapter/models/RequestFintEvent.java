package no.fintlabs.adapter.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a request to the adapter
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestFintEvent implements FintEvent {
    /**
     * GUID for correlation ID. The same ID should follow the request both upstream and downstream.
     */
    String corrId;

    /**
     * OrgId for the current customer.
     */
    private String orgId;

    /**
     * Name of the FINT domain. E.g. utdanning.
     */
    private String domainName;

    /**
     * Name of the FINT package. E.g. vurdering.
     */
    private String packageName;

    /**
     * Name of the FINT class/entity. E.g. fravar
     */
    private String resourceName;

    /**
     * The type of operation to be performed (CREATE, UPDATE)
     */
    private OperationType operationType;

    /**
     * When the event was created.
     */
    private long created;

    /**
     * How long the request is valid in milliseconds
     */
    private long timeToLive;

    /**
     * The object to which the event applies
     */
    private String value;
}
