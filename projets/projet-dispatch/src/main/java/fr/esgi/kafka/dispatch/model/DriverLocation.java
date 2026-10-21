package fr.esgi.kafka.dispatch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Ping de position chauffeur (topic dispatch.driver.locations,
 * cle = driver_id, un ping toutes les ~8 s par chauffeur).
 * status : AVAILABLE | BUSY | OFFLINE.
 */
public record DriverLocation(
        @JsonProperty("driver_id") String driverId,
        @JsonProperty("lat") Double lat,
        @JsonProperty("lon") Double lon,
        @JsonProperty("zone") String zone,
        @JsonProperty("status") String status,
        @JsonProperty("timestamp") String timestamp) {
}
