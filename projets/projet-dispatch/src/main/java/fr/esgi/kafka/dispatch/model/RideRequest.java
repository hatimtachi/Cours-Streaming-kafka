package fr.esgi.kafka.dispatch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Demande de course (topic dispatch.ride.requests, cle = user_id). */
public record RideRequest(
        @JsonProperty("request_id") String requestId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("pickup") GeoPoint pickup,
        @JsonProperty("dropoff") GeoPoint dropoff,
        @JsonProperty("timestamp") String timestamp) {

    /** Point geographique avec sa zone de rattachement. */
    public record GeoPoint(
            @JsonProperty("lat") Double lat,
            @JsonProperty("lon") Double lon,
            @JsonProperty("zone") String zone) {
    }
}
