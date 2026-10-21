package fr.esgi.kafka.binge.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Fiche contenu (topic compacte binge.catalog, cle = content_id). */
public record CatalogItem(
        @JsonProperty("content_id") String contentId,
        @JsonProperty("title") String title,
        @JsonProperty("type") String type,
        @JsonProperty("genre") String genre,
        @JsonProperty("duration_seconds") Integer durationSeconds,
        @JsonProperty("released_year") Integer releasedYear) {
}
