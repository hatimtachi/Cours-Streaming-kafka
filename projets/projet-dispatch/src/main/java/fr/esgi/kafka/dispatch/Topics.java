package fr.esgi.kafka.dispatch;

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
    public static final String RIDE_REQUESTS = "dispatch.ride.requests";
    public static final String DRIVER_LOCATIONS = "dispatch.driver.locations";
    public static final String TRIP_EVENTS = "dispatch.trip.events";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE = ConfigProvider.getConfig()
            .getOptionalValue("GROUPE", String.class)
            .orElse("grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    // --- Sorties (a produire par votre application) ---
    public static final String DLQ = out("dispatch.dlq");
    public static final String DEMAND_BY_ZONE = out("dispatch.demand.by-zone");
    public static final String SUPPLY_BY_ZONE = out("dispatch.supply.by-zone");
    public static final String SURGE = out("dispatch.surge");
    public static final String ALERTS_TRIPS = out("dispatch.alerts.trips");
}
