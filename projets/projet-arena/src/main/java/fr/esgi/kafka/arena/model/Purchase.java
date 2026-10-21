package fr.esgi.kafka.arena.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Achat en jeu (topic arena.purchases, cle = player_id).
 * item_type : SKIN | MONNAIE | PASSE_COMBAT | LOT.
 * C'est le flux qui part en facturation : un achat n'est encaisse
 * qu'une fois.
 */
public record Purchase(
        @JsonProperty("purchase_id") String purchaseId,
        @JsonProperty("player_id") String playerId,
        @JsonProperty("item_id") String itemId,
        @JsonProperty("item_type") String itemType,
        @JsonProperty("amount_eur") Double amountEur,
        @JsonProperty("timestamp") String timestamp) {
}
