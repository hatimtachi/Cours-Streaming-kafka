package fr.esgi.kafka.arena;

import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Noms des topics du projet.
 * Entrees fournies par l'enseignant (lecture seule) ; sorties prefixees
 * par votre code groupe (variable d'environnement GROUPE, ex: grp07).
 */
public final class Topics {

    private Topics() {
    }

    // --- Entrees (fournies, ne pas ecrire dedans) ---
    public static final String MATCH_EVENTS = "arena.match.events";
    public static final String PURCHASES = "arena.purchases";
    public static final String PLAYERS = "arena.players";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE = ConfigProvider.getConfig()
            .getOptionalValue("GROUPE", String.class)
            .orElse("grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    // --- Sorties (a produire par votre application) ---
    public static final String DLQ = out("arena.dlq");
    public static final String HEADSHOT = out("arena.headshot");
    public static final String TOP_WEAPONS = out("arena.top.weapons");
    public static final String REVENUE = out("arena.revenue");
    public static final String SESSIONS = out("arena.sessions");
}
