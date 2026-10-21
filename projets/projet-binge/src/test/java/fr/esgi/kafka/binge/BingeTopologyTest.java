package fr.esgi.kafka.binge;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (bonus "tests").
 * Outil : org.apache.kafka.streams.TopologyTestDriver (deja en scope test).
 * Demarche vue en seance 14 : construire la topologie, creer des
 * TestInputTopic / TestOutputTopic, injecter des messages, verifier.
 */
class BingeTopologyTest {

    @Test
    @Disabled("A implementer : un message invalide doit finir dans la DLQ")
    void messageInvalideDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : 30 BUFFERING en 1 min declenchent une alerte QoE")
    void bufferingStormDeclencheUneAlerte() {
    }
}
