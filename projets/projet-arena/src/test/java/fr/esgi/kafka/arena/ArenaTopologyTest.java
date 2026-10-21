package fr.esgi.kafka.arena;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (facultatif mais fortement
 * conseille, demarche vue en seance 14). Outil : TopologyTestDriver
 * (ajoutez kafka-streams-test-utils en scope test dans le pom).
 * On teste la Topology nue (new TopologyProducer().buildTopology()),
 * pas besoin de @QuarkusTest ni de broker.
 * Les fenetres de session se pilotent tres bien ici : il suffit
 * d'injecter deux evenements espaces de plus ou de moins que le trou.
 */
class ArenaTopologyTest {

    @Test
    @Disabled("A implementer : un evenement invalide doit finir dans la DLQ")
    void evenementInvalideDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : deux rafales espacees de 6 min font DEUX "
            + "sessions ; espacees de 4 min, une seule")
    void leTrouDinactiviteDelimiteLaSession() {
    }

    @Test
    @Disabled("A implementer : un doublon exact n'encaisse pas deux fois")
    void doublonNencaissePasDeuxFois() {
    }
}
