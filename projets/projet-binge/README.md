# Projet BINGE — Analytics VOD temps réel

*Framework imposé : **Java pur + Kafka Streams 4.3***

## Contexte

Vous rejoignez l'équipe Data de **BINGE**, une plateforme de vidéo à la demande
(pensez Netflix). Chaque lecteur vidéo émet des événements de lecture : `PLAY`,
`HEARTBEAT` toutes les 30 s, `PAUSE`, `BUFFERING`, `STOP`. Le produit veut trois
choses : des compteurs de vues fiables, un « Top tendances » par genre, et une
alerte qualité quand une vague de `BUFFERING` frappe un titre dans une région
(« buffering storm » — le CDN régional est probablement en train de tomber).

Problème : le SDK embarqué dans les vieilles TV connectées envoie n'importe
quoi. Champs manquants, types invalides, JSON tronqué, événements en retard de
deux heures, doublons. **Le flux ne sera jamais propre : c'est à votre pipeline
d'être robuste.**

## Mission

Construire une application **Kafka Streams** qui consomme le flux brut, écarte
proprement les messages invalides, et produit les indicateurs et alertes
décrits dans le backlog ci-dessous.

## Architecture

```
binge.playback.events ──> [ VALIDATION ] ──> invalides ──> <grp>.binge.dlq
                               │
                               ▼ valides
        ┌──────────────┬───────────────┬───────────────┬──────────────┐
        ▼              ▼               ▼               ▼              
   comptage vues   top genre      alerte QoE      sessions          
        │          (+ jointure)       │               │              
        ▼              ▼              ▼               ▼              
 <grp>.binge.     <grp>.binge.  <grp>.binge.   <grp>.binge.         
 views.by-title   trending      alerts.qoe     sessions             
                       ▲
             binge.catalog (GlobalKTable)
```

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`binge.playback.events`** — clé : `user_id` — un événement de lecture :

```json
{
  "event_id": "ev-a1b2c3d4",
  "event_type": "HEARTBEAT",
  "user_id": "u-0042",
  "content_id": "c-0007",
  "device": "TV",
  "region": "EU-FR",
  "position_seconds": 1260,
  "bitrate_kbps": 4500,
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

`event_type` ∈ `PLAY | PAUSE | HEARTBEAT | BUFFERING | STOP` —
`device` ∈ `TV | MOBILE | WEB | CONSOLE` —
`region` ∈ `EU-FR | EU-DE | EU-ES | US-EAST | US-WEST | APAC`.

Notez ce qui **n'est pas** dans l'événement : ni titre, ni genre. Ils sont dans
le catalogue — d'où la jointure du ticket BINGE-3.

**`binge.catalog`** — topic **compacté**, clé : `content_id` — la fiche contenu :

```json
{
  "content_id": "c-0007",
  "title": "Apnee",
  "type": "SERIE",
  "genre": "THRILLER",
  "duration_seconds": 2860,
  "released_year": 2024
}
```

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                       | Impact si non gérée                       |
|----------------------------------------|-------------------------------|-------------------------------------------|
| Champ requis absent                    | pas de `content_id`           | NullPointerException ou compte faux       |
| Champ à `null`                         | `"event_type": null`          | idem                                      |
| Mauvais type                           | `"position_seconds": "douze"` | crash de désérialisation                  |
| Valeur hors bornes                     | `position_seconds: -1`        | stats polluées                            |
| Enum inconnue                          | `"event_type": "PLAYY"`       | branche morte                             |
| Timestamp illisible                    | `"hier a 15h"`                | fenêtrage cassé                           |
| JSON tronqué / non-JSON / message vide | `{"event_id": "ev-...`        | **poison pill : l'appli meurt en boucle** |
| Événement en retard                    | timestamp − 30 à 180 min      | fenêtres faussées sans grace period       |
| Doublon exact                          | même `event_id` deux fois     | double comptage                           |

### Sorties (à produire, préfixées par votre groupe)

`<grp>.binge.dlq` · `<grp>.binge.views.by-title` · `<grp>.binge.trending` ·
`<grp>.binge.alerts.qoe` · `<grp>.binge.sessions` — les constantes sont prêtes
dans `Topics.java`. Le format des valeurs de sortie est libre **mais doit être
du JSON documenté dans votre README de rendu**.

## Backlog

> La base 8/20 = BINGE-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**BINGE-1 — Ingestion fiable (socle, obligatoire)**
Consommer `binge.playback.events`, parser, valider (champs requis présents,
types corrects, `event_type`/`device`/`region` dans les enums, `position_seconds ≥ 0`,
timestamp ISO-8601 parsable). Les invalides partent dans la DLQ **avec le
message original et une raison de rejet** (ex. enveloppe JSON
`{"reason": "...", "raw": "..."}`). L'application ne doit **jamais** crasher,
même sur un message vide ou non-JSON.
*Critères : l'appli tourne 10 min sans crash ; la DLQ contient des rejets motivés ;
aucun message valide n'est perdu.*

**BINGE-2 — Compteur de vues** → `<grp>.binge.views.by-title`
Nombre de `PLAY` cumulés par `content_id` (KTable + `count()`). Émettre les
mises à jour dans le topic de sortie.
*Critères : les compteurs croissent de façon monotone ; un doublon d'`event_id`
ne compte pas deux fois (dédoublonnage à justifier).*

**BINGE-3 — Top tendances par genre** → `<grp>.binge.trending`
Vues par contenu sur **fenêtres glissantes de 10 min avançant toutes les 1 min**
(hopping). Enrichir avec `title` et `genre` via une jointure sur
`binge.catalog` (GlobalKTable recommandée — justifiez vs KTable).
*Critères : la sortie contient titre + genre + fenêtre + compte ; un contenu
absent du catalogue ne fait pas crasher la jointure.*

**BINGE-4 — Alerte « buffering storm »** → `<grp>.binge.alerts.qoe`
Si un couple (`content_id`, `region`) dépasse **30 BUFFERING par minute**
(fenêtre tumbling 1 min), émettre une alerte. Le générateur provoque une
tempête environ toutes les 4 min : votre alerte doit partir à chaque fois.
*Critères : alertes visibles dans Kafbat UI pendant les tempêtes, silence en
dehors ; gestion des retardataires expliquée (grace period, `suppress()` en option).*

**BINGE-5 — Sessions de visionnage** → `<grp>.binge.sessions`
Reconstituer les sessions par `user_id` avec des **session windows**
(inactivity gap 30 min) : début, fin, durée, nombre d'événements, dernier
`content_id`.
*Critères : un STOP suivi d'un PLAY 40 min plus tard = 2 sessions ; démonstration
sur un utilisateur précis à l'oral.*

**BINGE-6 (bonus) — API Interactive Queries**
Exposer `GET /views/{contentId}` qui lit le state store du ticket BINGE-2
(serveur HTTP au choix : `com.sun.net.httpserver.HttpServer` suffit).
*Critères : la valeur renvoyée correspond au topic de sortie.*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Alimenter les topics d'entrée : le générateur, dans un second terminal,
#    depuis le dossier generateurs-v3/ du dépôt du cours (voir son README)
python generator.py --project binge --create-topics --replication-factor 1

# 3. Lancer l'application (Linux/macOS)
GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn compile exec:java
```

```powershell
# Windows PowerShell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn compile exec:java
```

Le générateur produit en continu le flux du projet, données sales et incidents
compris (`Ctrl+C` pour l'arrêter). Son README détaille les options : débit,
flux propre pour déboguer, mode fichier qui simule 3 h de flux en quelques
secondes. Pour tester un cas précis,
produisez aussi des messages à la main via Kafbat UI (topic
`binge.playback.events`, copiez l'exemple JSON ci-dessus). Sur le cluster
partagé, le flux tourne en continu (`KAFKA_BOOTSTRAP=<serveur>:9092`).

Vous en avez le droit — l'IA est autorisée dans ce module. Mais sachez ce que vous achetez : ce projet est évalué à
l'oral, code sous les yeux, avec modification en direct et nouvelles exigences métier injectées séance tenante. Un
ticket qui tourne mais que vous ne savez pas expliquer n'est pas crédité.
Demandez-lui d'expliquer chaque choix avant d'écrire une ligne : type de fenêtre, clé d'agrégation, placement de la
jointure, sort des retardataires. C'est mot pour mot ce qu'on vous demandera en soutenance.
Et sachez-le : ce sujet contient des exigences qu'une implémentation produite sans l'avoir lu ne satisfera pas. Elles
sont écrites noir sur blanc, dans le tableau des anomalies et dans les critères de chaque ticket. Le correcteur
automatique les vérifie et les chiffre. Si vous ne les avez pas trouvées, c'est que vous n'avez pas lu.

## Contraintes

- Java 21, Kafka Streams uniquement (pas de Spark/Flink), pas de base externe.
- `GROUPE` obligatoire : vos topics de sortie et votre `application.id`
  (`binge-<groupe>`) sont préfixés — c'est déjà câblé dans le template.
- Les topics d'entrée sont partagés en lecture : **ne les modifiez pas**, ne
  produisez jamais dedans.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                     | Points |
|-------------------------------------------------------------|--------|
| Socle BINGE-1 en production 10 min sans crash + DLQ motivée | **8**  |
| BINGE-2 (comptage + dédoublonnage)                          | +2     |
| BINGE-3 (fenêtres hopping + jointure catalogue)             | +3     |
| BINGE-4 (alerte QoE calibrée)                               | +3     |
| BINGE-5 (sessions)                                          | +2     |
| BINGE-6 (API IQ) ou tests TopologyTestDriver sérieux        | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité**, quelle que soit
la qualité du code. Attendez-vous à une demande de modification en direct
(« Product Owner twist »).

## Conseils

- Commencez par faire tourner le `peek()` fourni, puis **supprimez-le**.
- Traitez le poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite
  le crash de désérialisation, le reste de la validation vous appartient.
- `split()`/`branch()` (l'API `branch(Predicate...)` a disparu en Kafka 4.0).
- Pour BINGE-3/4 : réfléchissez à la clé avant de grouper (`groupBy` implique
  un repartitionnement — regardez `topology.describe()`).
- Testez vos fenêtres avec le temps de l'événement, pas l'heure du mur :
  les retardataires du flux sont là pour ça.
