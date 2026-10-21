package fr.esgi.kafka.arena.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Fiche joueur (topic compacte arena.players, cle = player_id). */
public record Player(
        @JsonProperty("player_id") String playerId,
        @JsonProperty("pseudo") String pseudo,
        @JsonProperty("region") String region,
        @JsonProperty("rank") String rank,
        @JsonProperty("level") Integer level) {
}
