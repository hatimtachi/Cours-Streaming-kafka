package fr.esgi.kafka.binge.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Evenement de lecture video (topic binge.playback.events).
 * Le format attendu est decrit dans le README, section Topics et formats.
 * Rappel : le flux contient des messages invalides - tout champ peut etre
 * absent, null ou d'un mauvais type.
 */
public record PlaybackEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("user_id") String userId,
        @JsonProperty("content_id") String contentId,
        @JsonProperty("device") String device,
        @JsonProperty("region") String region,
        @JsonProperty("position_seconds") Integer positionSeconds,
        @JsonProperty("bitrate_kbps") Integer bitrateKbps,
        @JsonProperty("timestamp") String timestamp) {
}
