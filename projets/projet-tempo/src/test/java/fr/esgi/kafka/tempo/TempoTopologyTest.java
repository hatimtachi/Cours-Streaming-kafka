package fr.esgi.kafka.tempo;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (bonus TMP-6, vu en seance 14).
 * Outil : TopologyTestDriver (ajoutez kafka-streams-test-utils en scope
 * test dans le pom, version alignee sur le client Kafka du BOM Quarkus).
 * On teste la Topology nue (new TopologyProducer().buildTopology()),
 * pas besoin de @QuarkusTest ni de broker.
 */
class TempoTopologyTest {

    @Test
    @Disabled("A implementer : un message invalide doit finir dans la DLQ")
    void messageInvalideDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : COMPLETE a 31000 ms credite 0.003 a l'artiste")
    void ecouteCompleteCrediteUneRoyaltie() {
    }

    @Test
    @Disabled("A implementer : SKIP a 12000 ms ne credite rien")
    void skipNeCrediteRien() {
    }
}
