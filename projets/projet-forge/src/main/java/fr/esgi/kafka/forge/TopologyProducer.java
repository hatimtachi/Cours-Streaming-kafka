package fr.esgi.kafka.forge;

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
 * Construisez ici la topologie du projet FORGE.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    /** Au-dela de ce taux de rebut, l'outil est en derive. */
    public static final double REBUT_MAX_RATIO = 0.15;
    /** En dessous de ce volume, le taux n'est pas significatif. */
    public static final int REBUT_MIN_PIECES = 50;

    /**
     * Minutes sans le moindre releve au-dela desquelles une machine est
     * declaree muette. Doit rester configurable sans recompiler :
     * l'enseignant la fera varier en soutenance.
     */
    @ConfigProperty(name = "SILENCE_MINUTES", defaultValue = "8")
    int silenceMinutes;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawReadings = builder.stream(
                Topics.READINGS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawProduction = builder.stream(
                Topics.PRODUCTION,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ces peek (ils polluent les logs et coutent cher).
        rawReadings.peek((k, v) -> Log.infof("[forge] releve %s -> %s", k, v));
        rawProduction.peek((k, v) -> Log.infof("[forge] piece %s -> %s", k, v));

        // -----------------------------------------------------------------
        // FOR-1 - Ingestion fiable
        //   Parser (model.Reading, model.ProducedUnit), valider les DEUX
        //   flux (champs requis, temperature_c / vibration_mm_s / rpm /
        //   cycle_time_ms / unit_value_eur numeriques et dans les bornes,
        //   scrap booleen, timestamp ISO-8601), router les invalides vers
        //   Topics.DLQ avec message original + raison.
        //   Pistes : split()/Branched, JsonSerdes.parseOrNull(...).
        //   Attention : en JSON, true n'est pas un nombre. Un
        //   "rpm": true doit partir en DLQ.
        // -----------------------------------------------------------------

        // FOR-2 - Derive d'outil (tumbling 10 min)        -> Topics.SCRAP
        //         Taux de rebut par machine. Derive si le taux depasse
        //         REBUT_MAX_RATIO ET que la cellule compte au moins
        //         REBUT_MIN_PIECES pieces. Ici le garde-fou sert
        //         vraiment : beaucoup de machines produisent peu.

        // FOR-3 - Top produits par atelier (hopping 15/5 min,
        //         jointure GlobalKTable forge.machines) -> Topics.TOP_BY_ATELIER
        //         L'atelier n'est pas dans l'evenement : il vient de la
        //         fiche machine, il faut donc enrichir AVANT de grouper.
        //         Des machines recemment installees ne sont pas encore au
        //         catalogue : leurs pieces doivent ressortir en atelier
        //         INCONNU, pas disparaitre.

        // FOR-4 - Valeur produite par machine             -> Topics.VALUE
        //         Cumul de unit_value_eur par machine_id (KTable,
        //         aggregate). Une piece au rebut ne vaut rien. Une piece
        //         n'est comptee qu'UNE fois.

        // FOR-5 - Machine muette                          -> Topics.ALERTS_SILENT
        //         Signaler toute machine dont le dernier releve remonte a
        //         plus de silenceMinutes.
        //         Attention : aucune fenetre du DSL n'emet quoi que ce
        //         soit sur une fenetre VIDE. Compter les releves et
        //         guetter un zero ne produira jamais rien. Il faut donc
        //         qu'une horloge vienne vous reveiller pour aller
        //         inspecter un etat. Cherchez du cote de la Processor API.
        //         Et rappelez-vous que la machine, elle, continue de
        //         produire : ce n'est pas l'absence de TOUT evenement
        //         qu'on traque, c'est l'absence de RELEVE.

        // FOR-6 (bonus) - Tests unitaires TopologyTestDriver (cf. README)

        return builder.build();
    }
}
