package fr.esgi.kafka.dispatch;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (bonus "tests").
 * Outil : TopologyTestDriver (ajoutez kafka-streams-test-utils en scope
 * test dans le pom, version alignee sur le client Kafka du BOM Quarkus).
 * On teste la Topology nue (new TopologyProducer().buildTopology()),
 * pas besoin de @QuarkusTest ni de broker.
 */
class DispatchTopologyTest {

    @Test
    @Disabled("A implementer : un ping GPS hors de Paris doit finir dans la DLQ")
    void pingHorsBornesDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : une course a 900 km/h doit lever une alerte")
    void courseAberranteDeclencheUneAlerte() {
    }
}
