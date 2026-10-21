package fr.esgi.kafka.dispatch;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Construisez ici la topologie du projet DISPATCH.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawRequests = builder.stream(
                Topics.RIDE_REQUESTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawLocations = builder.stream(
                Topics.DRIVER_LOCATIONS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawTrips = builder.stream(
                Topics.TRIP_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        rawLocations.peek((key, value) -> Log.infof("[dispatch] %s -> %s", key, value));

        // -----------------------------------------------------------------
        // DISP-1 - Ingestion fiable (les TROIS flux)
        //   Parser (model.RideRequest / DriverLocation / TripEvent), valider
        //   (dont bornes GPS Paris : lat 48.7-49.0, lon 2.2-2.5), router les
        //   invalides vers Topics.DLQ avec message original + raison.
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // DISP-2 - Demande par zone (tumbling 2 min)     -> Topics.DEMAND_BY_ZONE
        // DISP-3 - Offre par zone (KTable des chauffeurs) -> Topics.SUPPLY_BY_ZONE
        // DISP-4 - Surge pricing (demande/offre > 2)     -> Topics.SURGE
        // DISP-5 - Courses anormales (vitesse, duree)    -> Topics.ALERTS_TRIPS
        // DISP-6 (bonus) - API REST Interactive Queries : GET /api/v1/surge/{zone}

        return builder.build();
    }
}
