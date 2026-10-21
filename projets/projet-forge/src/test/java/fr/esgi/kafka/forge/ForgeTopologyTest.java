package fr.esgi.kafka.forge;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (bonus FOR-6, vu en seance 14).
 * Outil : TopologyTestDriver (ajoutez kafka-streams-test-utils en scope
 * test dans le pom, version alignee sur le client Kafka du BOM Quarkus).
 * On teste la Topology nue (new TopologyProducer().buildTopology()),
 * pas besoin de @QuarkusTest ni de broker.
 *
 * Le troisieme test est le plus interessant du projet : il faut faire
 * AVANCER LE TEMPS sans envoyer de message sur la machine surveillee.
 * Cherchez du cote de advanceWallClockTime, et demandez-vous ce qui fait
 * avancer le temps de votre punctuator.
 */
class ForgeTopologyTest {

    @Test
    @Disabled("A implementer : un releve invalide doit finir dans la DLQ")
    void releveInvalideDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : une piece au rebut ne vaut rien")
    void pieceAuRebutNeComptePasDansLaValeur() {
    }

    @Test
    @Disabled("A implementer : une machine sans releve depuis plus de "
            + "SILENCE_MINUTES est signalee ; une machine qui a emis il y a "
            + "1 min ne l'est pas")
    void machineSansReleveEstSignalee() {
    }
}
