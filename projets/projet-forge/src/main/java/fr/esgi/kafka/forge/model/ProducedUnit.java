package fr.esgi.kafka.forge.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Piece produite (topic forge.production, cle = machine_id).
 * scrap : la piece part au rebut. Une piece au rebut ne vaut rien.
 */
public record ProducedUnit(
        @JsonProperty("unit_id") String unitId,
        @JsonProperty("machine_id") String machineId,
        @JsonProperty("product_id") String productId,
        @JsonProperty("scrap") Boolean scrap,
        @JsonProperty("cycle_time_ms") Integer cycleTimeMs,
        @JsonProperty("unit_value_eur") Double unitValueEur,
        @JsonProperty("timestamp") String timestamp) {
}
