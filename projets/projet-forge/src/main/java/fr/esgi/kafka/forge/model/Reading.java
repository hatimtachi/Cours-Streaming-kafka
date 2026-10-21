package fr.esgi.kafka.forge.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Releve capteur (topic forge.readings, cle = machine_id).
 * Chaque machine emet un releve toutes les ~15 s, regulierement.
 * Son silence est donc une information a part entiere.
 */
public record Reading(
        @JsonProperty("reading_id") String readingId,
        @JsonProperty("machine_id") String machineId,
        @JsonProperty("temperature_c") Double temperatureC,
        @JsonProperty("vibration_mm_s") Double vibrationMmS,
        @JsonProperty("rpm") Integer rpm,
        @JsonProperty("timestamp") String timestamp) {
}
