package no.fintlabs.consumer.admin;

import java.util.Date;

public record CacheEntry(Date lastUpdated, Date lastFullSync, Integer size) {
}
