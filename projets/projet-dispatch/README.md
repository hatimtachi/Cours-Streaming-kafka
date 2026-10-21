# Projet DISPATCH — Plateforme VTC temps réel

*Framework imposé : **Quarkus 3.19 + Kafka Streams***

## Contexte

Vous rejoignez l'équipe Marketplace de **DISPATCH**, une plateforme de VTC
opérant sur Paris (pensez Uber). Trois flux arrivent en continu : les demandes
de course des passagers, les pings GPS des 80 chauffeurs (toutes les ~8 s), et
les événements de course (`START`/`END`). Le produit veut piloter le **surge
pricing** : quand la demande explose dans une zone par rapport au nombre de
chauffeurs disponibles (sortie de concert à Bercy…), le prix doit monter. Et
l'équipe fraude veut détecter les **courses fantômes** : 900 km en 20 minutes,
ou 6 heures de course pour 12 km.

Les GPS de certains téléphones envoient des coordonnées aberrantes, des champs
manquants, du JSON tronqué. **Le flux ne sera jamais propre : c'est à votre
pipeline d'être robuste.**

## Mission

Construire une application **Quarkus + Kafka Streams** qui consomme les trois
flux, écarte proprement les messages invalides, calcule l'offre et la demande
par zone, déclenche le surge et signale les courses anormales.

## Architecture

```
dispatch.ride.requests ────┐
dispatch.driver.locations ─┼─> [ VALIDATION ] ──> invalides ──> <grp>.dispatch.dlq
dispatch.trip.events ──────┘        │
                                    ▼ valides
              ┌───────────────┬─────┴─────────┬────────────────┐
              ▼               ▼               ▼                ▼
        demande/zone    offre/zone      surge (ratio)    courses anormales
        (tumbling 2min) (KTable)        demande/offre>2  (vitesse, durée)
              │               │               │                │
              ▼               ▼               ▼                ▼
        <grp>.dispatch. <grp>.dispatch. <grp>.dispatch.  <grp>.dispatch.
        demand.by-zone  supply.by-zone  surge            alerts.trips
```

Les 12 zones : `Z-CHATELET, Z-MONTMARTRE, Z-BASTILLE, Z-BERCY, Z-DEFENSE,
Z-ETOILE, Z-MONTPARNASSE, Z-BELLEVILLE, Z-ODEON, Z-PIGALLE, Z-NATION, Z-AUTEUIL`.

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`dispatch.ride.requests`** — clé : `user_id` :

```json
{
  "request_id": "req-398d1ca6",
  "user_id": "u-0278",
  "pickup": {
    "lat": 48.854972,
    "lon": 2.375735,
    "zone": "Z-BASTILLE"
  },
  "dropoff": {
    "lat": 48.848057,
    "lon": 2.363855,
    "zone": "Z-NATION"
  },
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

**`dispatch.driver.locations`** — clé : `driver_id` — un ping toutes les ~8 s
par chauffeur, `status` ∈ `AVAILABLE | BUSY | OFFLINE` :

```json
{
  "driver_id": "drv-034",
  "lat": 48.864121,
  "lon": 2.340588,
  "zone": "Z-CHATELET",
  "status": "AVAILABLE",
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

**`dispatch.trip.events`** — clé : `trip_id` — `event` ∈ `START | END` ;
`distance_km` et `duration_seconds` **uniquement sur les END** (ce sont les
valeurs réelles de la course ; le flux est accéléré, le END arrive quelques
dizaines de secondes après le START) :

```json
{
  "trip_id": "trip-74daaebf",
  "driver_id": "drv-015",
  "user_id": "u-0398",
  "event": "END",
  "distance_km": 7.42,
  "duration_seconds": 1260,
  "timestamp": "2026-07-04T14:05:41.203Z"
}
```

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                     | Impact si non gérée                       |
|----------------------------------------|-----------------------------|-------------------------------------------|
| Champ requis absent                    | pas de `zone`               | agrégat par zone faux                     |
| Champ à `null`                         | `"status": null`            | NullPointerException                      |
| Mauvais type                           | `"lat": "douze"`            | crash de désérialisation                  |
| GPS hors bornes                        | `lat: 91.2` ou hors Paris   | zones polluées                            |
| Enum inconnue                          | `"status": "AVAILABLEE"`    | offre surestimée                          |
| Timestamp illisible                    | `"hier a 15h"`              | fenêtrage cassé                           |
| JSON tronqué / non-JSON / message vide | `{"driver_id": "drv`        | **poison pill : l'appli meurt en boucle** |
| Événement en retard                    | timestamp − 30 à 180 min    | fenêtres faussées                         |
| Doublon exact                          | même `request_id` deux fois | demande double-comptée                    |

Bornes de validité GPS pour ce projet : `48.7 ≤ lat ≤ 49.0` et `2.2 ≤ lon ≤ 2.5`.

### Sorties (à produire, préfixées par votre groupe)

`<grp>.dispatch.dlq` · `<grp>.dispatch.demand.by-zone` ·
`<grp>.dispatch.supply.by-zone` · `<grp>.dispatch.surge` ·
`<grp>.dispatch.alerts.trips` — constantes prêtes dans `Topics.java`. Format
des valeurs libre **mais en JSON documenté dans votre README de rendu**.

## Backlog

> La base 8/20 = DISP-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**DISP-1 — Ingestion fiable (socle, obligatoire)**
Consommer les **trois** topics, parser, valider (champs requis, types, bornes
GPS ci-dessus, enums, timestamp ISO-8601 parsable). Invalides → DLQ **avec
message original + raison**. L'application ne doit **jamais** crasher.
*Critères : 10 min sans crash ; DLQ motivée ; aucun message valide perdu.*

**DISP-2 — Demande par zone** → `<grp>.dispatch.demand.by-zone`
Nombre de demandes par `pickup.zone` sur **fenêtres tumbling de 2 min**.
*Critères : clé = zone ; les rafales de Bercy (~toutes les 6 min) se voient
dans les comptes.*

**DISP-3 — Offre par zone** → `<grp>.dispatch.supply.by-zone`
Nombre de chauffeurs `AVAILABLE` par zone, basé sur la **dernière position
connue** de chaque chauffeur (KTable clée par `driver_id`, puis ré-agrégation
par zone — c'est le ticket subtil : un chauffeur qui change de zone doit être
décompté de l'ancienne).
*Critères : la somme des offres par zone ≤ 80 ; un chauffeur passé OFFLINE
disparaît de l'offre.*

**DISP-4 — Surge pricing** → `<grp>.dispatch.surge`
Croiser demande (DISP-2) et offre (DISP-3) par zone ; si
`demande / max(offre, 1) > 2`, émettre un événement de surge avec le ratio.
Le générateur provoque un surge à Bercy environ toutes les 6 min.
*Critères : chaque incident `surge_bercy` déclenche un surge sur Z-BERCY ;
silence ailleurs ; choix du type de jointure justifié.*

**DISP-5 — Courses anormales** → `<grp>.dispatch.alerts.trips`
Sur les `END` : vitesse moyenne = `distance_km / (duration_seconds/3600)`.
Alerte si **vitesse > 130 km/h** ou **duration_seconds > 10800** (3 h). Le
générateur injecte une course fantôme toutes les ~5 min.
*Critères : chaque `ghost_trip` est détectée avec la raison (vitesse ou durée) ;
pas de faux positifs sur le trafic normal (15–40 km/h).*

**DISP-6 (bonus) — API Interactive Queries**
Compléter `SurgeResource` : `GET /api/v1/surge/{zone}` lit le state store de
DISP-4 (`@Inject KafkaStreams` fourni par l'extension Quarkus).
*Critères : la valeur renvoyée correspond au topic surge ; 404 propre si la
zone est inconnue.*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Alimenter les topics d'entrée : le générateur, dans un second terminal,
#    depuis le dossier generateurs-v3/ du dépôt du cours (voir son README)
python generator.py --project dispatch --create-topics --replication-factor 1

# 3. Lancer en mode dev (live reload sur http://localhost:8090)
GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn quarkus:dev
```

```powershell
# Windows PowerShell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn quarkus:dev
```

Vous en avez le droit — l'IA est autorisée dans ce module. Mais sachez ce que vous achetez : ce projet est évalué à
l'oral, code sous les yeux, avec modification en direct et nouvelles exigences métier injectées séance tenante. Un
ticket qui tourne mais que vous ne savez pas expliquer n'est pas crédité.
Demandez-lui d'expliquer chaque choix avant d'écrire une ligne : type de fenêtre, clé d'agrégation, placement de la
jointure, sort des retardataires. C'est mot pour mot ce qu'on vous demandera en soutenance.
Et sachez-le : ce sujet contient des exigences qu'une implémentation produite sans l'avoir lu ne satisfera pas. Elles
sont écrites noir sur blanc, dans le tableau des anomalies et dans les critères de chaque ticket. Le correcteur
automatique les vérifie et les chiffre. Si vous ne les avez pas trouvées, c'est que vous n'avez pas lu.

**Important** : l'extension Quarkus attend que les topics listés dans
`quarkus.kafka-streams.topics` existent avant de démarrer la topologie :
lancez le générateur (étape 2, `--create-topics` la première fois) **avant**
l'application. Il produit en continu le flux du projet, données sales et incidents
compris (`Ctrl+C` pour l'arrêter). Son README détaille les options : débit,
flux propre pour déboguer, mode fichier qui simule 3 h de flux en quelques
secondes. Sur le cluster partagé, les topics existent déjà
(`KAFKA_BOOTSTRAP=<serveur>:9092`).

## Contraintes

- Java 21, Quarkus + Kafka Streams uniquement (pas de Spark/Flink), pas de
  base externe.
- `GROUPE` obligatoire : topics de sortie et `application.id`
  (`dispatch-<groupe>`) préfixés — déjà câblé.
- Topics d'entrée partagés en lecture : **ne les modifiez pas**.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                    | Points |
|------------------------------------------------------------|--------|
| Socle DISP-1 en production 10 min sans crash + DLQ motivée | **8**  |
| DISP-2 (demande par zone)                                  | +2     |
| DISP-3 (offre par zone, dernière position)                 | +3     |
| DISP-4 (surge, jointure demande × offre)                   | +3     |
| DISP-5 (courses anormales)                                 | +2     |
| DISP-6 (API IQ) ou tests TopologyTestDriver sérieux        | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité.** Attendez-vous à
une demande de modification en direct (« Product Owner twist »).

## Conseils

- Le `Log.infof` fourni sert au premier contact : supprimez-le ensuite.
- Poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite le crash,
  la validation métier vous appartient.
- DISP-3 : `groupBy((driverId, loc) -> loc.zone())` sur une **KTable** fait
  exactement le décompte « entrant/sortant » attendu — regardez la
  documentation de `KGroupedTable#count`.
- DISP-4 : deux tables avec des clés « zone » se joignent naturellement ;
  attention aux fenêtres côté demande (les types diffèrent).
- `split()`/`branch()` — l'ancienne API `branch(Predicate...)` a disparu en
  Kafka 4.0.
