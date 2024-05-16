package no.fintlabs.adapter.models;

import lombok.*;

import java.util.Set;

/**
 * Represent the contract between the adapter and FINT.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AdapterContract {
    /**
     * <p>
     * A <strong>persistent uniq id</strong> for the adapter. It should be in the format:
     * </p>
     * <p>
     * <code>https://&lt;company domain&gt;/&lt;customer domain&gt;/&lt;FINT domain&gt;/&lt;FINT package (optional)&gt;/&lt;UUID (optional)&gt;</code>
     * </p>
     * <p>
     * E.g.
     *  <ul>
     *      <li><code>https://visma.com/rogfk.no/utdanning</code></li>
     *      <li><code>https://visma.com/rogfk.no/utdanning/elev</code></li>
     *      <li><code>https://visma.com/rogfk.no/utdanning/elev/b93ffb1e-2b86-4709-aa5d-03453152d9a4</code></li>
     *  </ul>
     * </p>
     */
    private String adapterId;
    /**
     * <p>
     * OrgId for the customer this contract represent.
     * </p>
     * <p>
     * E.g. <code>rogfk.no</code>
     * </p>
     */
    private String orgId;
    /**
     * <p>
     * Username used for authentication to FINT.
     * </p>
     * <p>
     * E.g. `vis@adapter.rogfk.no`
     * </p>
     */
    private String username;
    /**
     * Interval the adapter should send heartbeats to FINT in minutes.
     */
    private int heartbeatIntervalInMinutes;
    /**
     * <p>
     * List of capablilities for the adapter.
     * </p>
     *
     * @see AdapterCapability
     */
    private Set<AdapterCapability> capabilities;
    /**
     * The registration time for the contract in Unix timestamp.
     */
    private long time;
}
