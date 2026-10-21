package fr.esgi.kafka.binge;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Construisez ici la topologie du projet BINGE.
 * Chaque ticket du backlog (README, section Backlog) correspond a un
 * bloc a implementer ci-dessous. Rien n'est code pour vous : les
 * commentaires rappellent seulement l'objectif et les APIs candidates.
 */
public final class BingeTopology {

    private BingeTopology() {
    }

    public static void build(StreamsBuilder builder) {

        // Flux brut : la valeur est du JSON... quand tout va bien.
        KStream<String, String> raw = builder.stream(
                Topics.PLAYBACK_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        raw.peek((key, value) -> System.out.println("[binge] " + key + " -> " + value));

        // -----------------------------------------------------------------
        // BINGE-1 - Ingestion fiable
        //   Parser chaque message (model.PlaybackEvent), le valider, puis
        //   router : valides -> suite du pipeline, invalides -> Topics.DLQ
        //   (conserver le message original et ajouter la raison du rejet).
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // BINGE-2 - Compteur de vues par contenu        -> Topics.VIEWS_BY_TITLE
        // BINGE-3 - Top tendances par genre (fenetres + jointure catalogue)
        //                                               -> Topics.TRENDING
        // BINGE-4 - Alerte qualite "buffering storm"    -> Topics.ALERTS_QOE
        // BINGE-5 - Sessions utilisateur (session windows) -> Topics.SESSIONS
        // BINGE-6 (bonus) - API REST Interactive Queries (cf. README)
    }
}
