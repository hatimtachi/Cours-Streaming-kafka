package fr.esgi.kafka.arena;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Construisez ici la topologie du projet ARENA.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    /** Au-dela de ce taux de headshots, le joueur vise trop bien. */
    public static final double HEADSHOT_MAX_RATIO = 0.80;
    /** En dessous de ce volume, le taux n'est pas significatif. */
    public static final int HEADSHOT_MIN_KILLS = 50;

    /**
     * Trou d'inactivite qui ferme une session de jeu, en minutes.
     * C'est la regle metier : cinq minutes sans le moindre evenement et le
     * joueur est considere comme parti.
     */
    @ConfigProperty(name = "SESSION_GAP_MINUTES", defaultValue = "5")
    int sessionGapMinutes;

    /**
     * Duree de session au-dela de laquelle on alerte, en minutes.
     * En production ce seuil serait a plusieurs heures ; il est court ici
     * pour que la demo tienne dans une seance. Doit rester configurable
     * sans recompiler : l'enseignant le fera varier en soutenance.
     */
    @ConfigProperty(name = "MARATHON_MINUTES", defaultValue = "12")
    int marathonMinutes;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawEvents = builder.stream(
                Topics.MATCH_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawPurchases = builder.stream(
                Topics.PURCHASES,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ces peek (ils polluent les logs et coutent cher).
        rawEvents.peek((k, v) -> Log.infof("[arena] event %s -> %s", k, v));
        rawPurchases.peek((k, v) -> Log.infof("[arena] achat %s -> %s", k, v));

        // -----------------------------------------------------------------
        // ARN-1 - Ingestion fiable
        //   Parser (model.MatchEvent, model.Purchase), valider les DEUX
        //   flux (champs requis, event_type/map/mode et item_type dans les
        //   enums, headshot booleen, reaction_ms numerique >= 0,
        //   amount_eur numerique dans les bornes, timestamp ISO-8601),
        //   router les invalides vers Topics.DLQ avec message original
        //   + raison.
        //   Pistes : split()/Branched, JsonSerdes.parseOrNull(...).
        //   Attention : en JSON, true n'est pas un nombre. Un
        //   "reaction_ms": true doit partir en DLQ.
        // -----------------------------------------------------------------

        // ARN-2 - Taux de headshot par joueur (tumbling 10 min) -> Topics.HEADSHOT
        //         Sur les KILL uniquement : headshots / kills par joueur.
        //         Triche si le taux depasse HEADSHOT_MAX_RATIO ET que la
        //         cellule compte au moins HEADSHOT_MIN_KILLS kills.
        //         La sortie DOIT porter window_start (cf. ARN-6).

        // ARN-3 - Top armes par region (hopping 15/5 min,
        //         jointure GlobalKTable arena.players)    -> Topics.TOP_WEAPONS
        //         La region vient de la fiche joueur : il faut enrichir
        //         AVANT de grouper. Des comptes jouent sans figurer au
        //         catalogue : leurs kills doivent ressortir en INCONNU,
        //         pas disparaitre.

        // ARN-4 - Chiffre d'affaires par joueur           -> Topics.REVENUE
        //         Cumul de amount_eur par player_id (KTable, aggregate).
        //         Un achat n'est encaisse qu'UNE fois.

        // ARN-5 - Sessions de jeu                         -> Topics.SESSIONS
        //         Fenetres de session : trou de sessionGapMinutes. Il n'y a
        //         pas d'identifiant de session dans le flux, c'est le trou
        //         qui la definit. Emettre player_id, session_start,
        //         session_end, duration_seconds, events, et marquer les
        //         sessions de plus de marathonMinutes.

        // ARN-6 - Resultat final de fenetre
        //         Aujourd'hui ARN-2 reemet son agregat a chaque kill. Ne
        //         publier que le resultat final de chaque fenetre.

        return builder.build();
    }
}
