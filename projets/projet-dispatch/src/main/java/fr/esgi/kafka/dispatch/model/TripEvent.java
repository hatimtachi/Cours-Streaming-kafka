package fr.esgi.kafka.dispatch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Evenement de course (topic dispatch.trip.events, cle = trip_id).
 * event : START | END. distance_km et duration_seconds ne sont presents
 * que sur les END.
 */
public record TripEvent(
        @JsonProperty("trip_id") String tripId,
        @JsonProperty("driver_id") String driverId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("event") String event,
        @JsonProperty("distance_km") Double distanceKm,
        @JsonProperty("duration_seconds") Integer durationSeconds,
        @JsonProperty("timestamp") String timestamp) {
}
