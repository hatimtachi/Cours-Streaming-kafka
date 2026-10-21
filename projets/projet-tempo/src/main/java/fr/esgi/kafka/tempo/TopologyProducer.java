package fr.esgi.kafka.tempo;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Construisez ici la topologie du projet TEMPO.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    /** Une ecoute complete (>= 30 s) rapporte 0.003 EUR a l'artiste. */
    public static final double ROYALTY_PER_STREAM = 0.003;
    public static final int MIN_MS_FOR_ROYALTY = 30000;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawEvents = builder.stream(
                Topics.LISTENING_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        rawEvents.peek((key, value) -> Log.infof("[tempo] %s -> %s", key, value));

        // -----------------------------------------------------------------
        // TMP-1 - Ingestion fiable
        //   Parser (model.ListeningEvent), valider (champs requis, types,
        //   event_type/country dans les enums, ms_played >= 0, timestamp
        //   ISO-8601), router les invalides vers Topics.DLQ avec message
        //   original + raison.
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // TMP-2 - Skip rate par titre (tumbling 10 min)   -> Topics.SKIP_RATE
        // TMP-3 - Top titres par pays (hopping 15/5 min,
        //         jointure GlobalKTable tempo.tracks)     -> Topics.TOP_BY_COUNTRY
        // TMP-4 - Compteur de royalties par artiste       -> Topics.ROYALTIES
        // TMP-5 - Fraude "stream farm"                    -> Topics.ALERTS_FRAUD
        // TMP-6 (bonus) - Tests unitaires TopologyTestDriver (cf. README)

        return builder.build();
    }
}
