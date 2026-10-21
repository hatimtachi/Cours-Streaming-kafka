package fr.esgi.kafka.forge.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Fiche machine (topic compacte forge.machines, cle = machine_id). */
public record Machine(
        @JsonProperty("machine_id") String machineId,
        @JsonProperty("label") String label,
        @JsonProperty("atelier") String atelier,
        @JsonProperty("ligne") String ligne,
        @JsonProperty("modele") String modele) {
}
