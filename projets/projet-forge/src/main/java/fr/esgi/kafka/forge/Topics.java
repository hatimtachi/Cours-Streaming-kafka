package fr.esgi.kafka.forge;

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
    public static final String READINGS = "forge.readings";
    public static final String PRODUCTION = "forge.production";
    public static final String MACHINES = "forge.machines";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE = ConfigProvider.getConfig()
            .getOptionalValue("GROUPE", String.class)
            .orElse("grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    // --- Sorties (a produire par votre application) ---
    public static final String DLQ = out("forge.dlq");
    public static final String SCRAP = out("forge.scrap");
    public static final String TOP_BY_ATELIER = out("forge.top.by-atelier");
    public static final String VALUE = out("forge.value");
    public static final String ALERTS_SILENT = out("forge.alerts.silent");
}
