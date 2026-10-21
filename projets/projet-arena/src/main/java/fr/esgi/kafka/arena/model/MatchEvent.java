package fr.esgi.kafka.arena.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Evenement de partie (topic arena.match.events, cle = player_id).
 * event_type : MATCH_START | KILL | DEATH | MATCH_END.
 * headshot et reaction_ms ne portent du sens que sur un KILL
 * (false / 0 partout ailleurs).
 * Il n'y a AUCUN identifiant de session dans ce flux : c'est le trou
 * d'inactivite qui delimite une session de jeu.
 */
public record MatchEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("match_id") String matchId,
        @JsonProperty("player_id") String playerId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("weapon") String weapon,
        @JsonProperty("headshot") Boolean headshot,
        @JsonProperty("reaction_ms") Integer reactionMs,
        @JsonProperty("map") String map,
        @JsonProperty("mode") String mode,
        @JsonProperty("timestamp") String timestamp) {
}
