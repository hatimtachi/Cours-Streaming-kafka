package fr.esgi.kafka.binge;

/**
 * Noms des topics du projet.
 *
 * Cluster partage : les topics d'ENTREE sont fournis par l'enseignant
 * (lecture seule, chaque groupe a son propre consumer group).
 * Les topics de SORTIE sont prefixes par votre code groupe (variable
 * d'environnement GROUPE, ex: grp07) pour ne pas ecraser le travail
 * des autres groupes.
 */
public final class Topics {

    private Topics() {
    }

    // --- Entrees (fournies, ne pas ecrire dedans) ---
    public static final String PLAYBACK_EVENTS = "binge.playback.events";
    public static final String CATALOG = "binge.catalog";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE =
            System.getenv().getOrDefault("GROUPE", "grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    // --- Sorties (a produire par votre application) ---
    public static final String DLQ = out("binge.dlq");
    public static final String VIEWS_BY_TITLE = out("binge.views.by-title");
    public static final String TRENDING = out("binge.trending");
    public static final String ALERTS_QOE = out("binge.alerts.qoe");
    public static final String SESSIONS = out("binge.sessions");
}
