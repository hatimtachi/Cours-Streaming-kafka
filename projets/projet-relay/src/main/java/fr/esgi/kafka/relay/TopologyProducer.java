package fr.esgi.kafka.relay;

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
 * Construisez ici la topologie du projet RELAY.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    /** Une banniere sous ce taux de visibilite rend le site suspect. */
    public static final double VIEWABILITY_MIN_RATIO = 0.40;
    /** En dessous de ce volume, le taux n'est pas significatif. */
    public static final int VIEWABILITY_MIN_IMPRESSIONS = 50;
    /** Un humain ne clique pas en moins d'une seconde. */
    public static final long BOT_MAX_DELAY_MS = 1000;
    /** Nombre de clics rapides a partir duquel on alerte. */
    public static final int BOT_MIN_CLICKS = 10;

    /**
     * Taille de la fenetre anti-robot, en minutes. Doit rester
     * configurable sans recompiler : l'enseignant la fera varier en
     * soutenance (BOT_WINDOW_MINUTES=5 par exemple).
     */
    @ConfigProperty(name = "BOT_WINDOW_MINUTES", defaultValue = "10")
    int botWindowMinutes;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawImpressions = builder.stream(
                Topics.IMPRESSIONS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawClicks = builder.stream(
                Topics.CLICKS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ces peek (ils polluent les logs et coutent cher).
        rawImpressions.peek((k, v) -> Log.infof("[relay] impression %s -> %s", k, v));
        rawClicks.peek((k, v) -> Log.infof("[relay] clic %s -> %s", k, v));

        // -----------------------------------------------------------------
        // RLY-1 - Ingestion fiable
        //   Parser (model.Impression, model.Click), valider les DEUX flux
        //   (champs requis, device/geo dans les enums, viewable booleen,
        //   paid_price_eur numerique entre 0 et 1, timestamp ISO-8601),
        //   router les invalides vers Topics.DLQ avec message original
        //   + raison.
        //   Pistes : split()/Branched, JsonSerdes.parseOrNull(...).
        //   Attention : en JSON, true n'est pas un nombre. Un
        //   "paid_price_eur": true doit partir en DLQ.
        // -----------------------------------------------------------------

        // RLY-2 - Viewability par site (tumbling 10 min)  -> Topics.VIEWABILITY
        //         Ratio viewable / total par site_id. Suspect si le ratio
        //         est sous VIEWABILITY_MIN_RATIO ET que la cellule compte
        //         au moins VIEWABILITY_MIN_IMPRESSIONS impressions.

        // RLY-3 - Top campagnes par pays (hopping 15/5 min,
        //         jointure GlobalKTable relay.campaigns) -> Topics.TOP_BY_GEO
        //         Certaines campagnes servies sont ABSENTES du catalogue :
        //         elles doivent survivre a la jointure.

        // RLY-4 - Depense par campagne                    -> Topics.SPEND
        //         Cumul de paid_price_eur par campaign_id (KTable,
        //         aggregate). Une impression n'est facturee qu'UNE fois.

        // RLY-5 - Tunnel impression -> clic, clics robots -> Topics.ALERTS_CLICKBOT
        //         Jointure flux-flux sur impression_id (les deux topics
        //         sont clees par user_id : il faut re-clef). Un clic a
        //         moins de BOT_MAX_DELAY_MS de son impression est un clic
        //         robot ; alerter au-dela de BOT_MIN_CLICKS clics robots
        //         par user sur une fenetre de botWindowMinutes.

        // RLY-6 (bonus) - Tests unitaires TopologyTestDriver (cf. README)

        return builder.build();
    }
}
