# Projet ARENA — Analytics jeu compétitif en ligne

*Framework imposé : **Quarkus 3.19 + Kafka Streams***

## Contexte

Vous rejoignez l'équipe Data d'**ARENA**, un jeu de tir compétitif en ligne
(pensez Riot Games). Chaque partie émet un flux d'événements : `MATCH_START`,
puis des `KILL` et des `DEATH`, puis `MATCH_END`. En parallèle, la boutique
émet les achats — skins, passes de combat, monnaie du jeu.

Quatre enjeux métier. L'**anti-triche** d'abord : un *aimbot* vise à votre
place, et ça se voit — un joueur humain plafonne autour de 20 % de headshots
et met 200 ms à réagir, un robot est à 95 % et 80 ms. Le **top armes par
région** ensuite, dont l'équilibrage se sert pour nerfer ce qui domine. Le
**chiffre d'affaires par joueur** surtout, parce que c'est ce qui part en
facturation : un achat compté deux fois, c'est un joueur débité deux fois. Et
enfin les **sessions de jeu**, que la conformité réclame pour les dispositifs
anti-addiction.

Sur ce dernier point, attention : **il n'y a aucun identifiant de session dans
le flux**. Une session, c'est ce qui se passe entre deux trous. Cinq minutes
sans le moindre événement et le joueur est parti.

Les clients de jeu envoient des messages cassés : champs manquants, types
invalides, JSON tronqué, doublons, retardataires. **Le flux ne sera jamais
propre : c'est à votre pipeline d'être robuste.**

## Mission

Construire une application **Quarkus + Kafka Streams** qui consomme les
événements de partie et les achats, écarte proprement les messages invalides,
et produit détections de triche, tops par région, chiffre d'affaires et
sessions de jeu.

## Architecture

```
arena.match.events ──┐
                     ├──> [ VALIDATION ] ──> invalides ──> <grp>.arena.dlq
arena.purchases ─────┘          │
                                ▼ valides
       ┌───────────────┬────────┴──────┬─────────────────┐
       ▼               ▼               ▼                 ▼
   headshots       top armes          CA            sessions de jeu
   (tumbling      (hopping 15/5    (cumul par      (fenêtres de session,
    10 min)        + jointure)      joueur)         trou de 5 min)
       │               ▲               │                 │
       ▼               │               ▼                 ▼
 <grp>.arena.    arena.players   <grp>.arena.      <grp>.arena.
 headshot        (GlobalKTable)  revenue           sessions
                       │
                       ▼
              <grp>.arena.top.weapons
```

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`arena.match.events`** — clé : `player_id` — `event_type` ∈
`MATCH_START | KILL | DEATH | MATCH_END`, `mode` ∈
`CLASSE | PARTIE_RAPIDE | MATCH_A_MORT` :

```json
{
  "event_id": "ev-3b8faa18",
  "match_id": "m-77097749",
  "player_id": "p-0374",
  "event_type": "KILL",
  "weapon": "Vandale",
  "headshot": true,
  "reaction_ms": 287,
  "map": "Ascension",
  "mode": "CLASSE",
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

`headshot` et `reaction_ms` ne portent du sens que sur un `KILL` (`false` / `0`
partout ailleurs).

**`arena.purchases`** — clé : `player_id` — `item_type` ∈
`SKIN | MONNAIE | PASSE_COMBAT | LOT` :

```json
{
  "purchase_id": "pur-9a1f0c33",
  "player_id": "p-0374",
  "item_id": "itm-0117",
  "item_type": "SKIN",
  "amount_eur": 12.99,
  "timestamp": "2026-07-04T14:05:02.104Z"
}
```

**`arena.players`** — topic **compacté**, clé : `player_id` — la fiche joueur.
C'est la seule source de la `region` :

```json
{
  "player_id": "p-0374",
  "pseudo": "NovaKane42",
  "region": "EU-OUEST",
  "rank": "PLATINE",
  "level": 142
}
```

`region` ∈ `EU-OUEST | EU-EST | NA | BR | KR | APAC`.

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                               | Impact si non gérée                                                     |
|----------------------------------------|---------------------------------------|-------------------------------------------------------------------------|
| Champ requis absent                    | pas de `player_id`                    | événement non attribuable                                               |
| Champ à `null`                         | `"event_type": null`                  | NullPointerException                                                    |
| Mauvais type                           | `"reaction_ms": "douze"`              | crash de désérialisation                                                |
| Mauvais type piégeux                   | `"reaction_ms": true`                 | en JSON, `true` n'est pas un nombre                                     |
| Valeur hors bornes                     | `amount_eur: 1000000000`              | CA absurde                                                              |
| Enum inconnue                          | `"event_type": "KILLL"`               | kill perdu                                                              |
| Timestamp illisible                    | `"hier a 15h"`                        | fenêtrage cassé                                                         |
| JSON tronqué / non-JSON / message vide | `{"event_id": "ev`                    | **poison pill : l'appli meurt en boucle**                               |
| Événement en retard                    | timestamp − 30 à 180 min              | **hors de toute grâce raisonnable : à écarter du fenêtrage, pas du CA** |
| Doublon exact                          | même `purchase_id` deux fois          | **joueur débité deux fois**                                             |
| Compte hors catalogue                  | `smurf-0007` absent d'`arena.players` | kills perdus si `join` au lieu de `leftJoin`                            |

### Sorties (à produire, préfixées par votre groupe)

`<grp>.arena.dlq` · `<grp>.arena.headshot` · `<grp>.arena.top.weapons` ·
`<grp>.arena.revenue` · `<grp>.arena.sessions` — constantes prêtes dans
`Topics.java`. Format des valeurs libre **mais en JSON documenté dans votre
README de rendu**, à deux exceptions près, imposées parce qu'elles sont
vérifiées : `<grp>.arena.headshot` doit porter `window_start`, et
`<grp>.arena.sessions` doit porter `duration_seconds` et `events`.

## Backlog

> La base 8/20 = ARN-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**ARN-1 — Ingestion fiable (socle, obligatoire)**
Consommer `arena.match.events` **et** `arena.purchases`, parser, valider
(champs requis, enums, `headshot` booléen, `reaction_ms ≥ 0`, `amount_eur`
dans les bornes, timestamp ISO-8601 parsable). Invalides → DLQ **avec message
original + raison**. L'application ne doit **jamais** crasher.
*Critères : 10 min sans crash ; DLQ motivée ; aucun message valide perdu.*

**ARN-2 — Détection d'aimbot** → `<grp>.arena.headshot`
Sur les `KILL` uniquement, en **fenêtres tumbling de 10 min** :
`headshots / kills` par `player_id`. Marquer les tricheurs : taux > 80 %
**avec au moins 50 kills** (sinon le ratio n'est pas significatif). Le
générateur lance un aimbot toutes les ~5 min. **La sortie doit porter
`window_start`** — ARN-6 s'en sert.
*Critères : chaque `aimbot` ressort marqué ; un joueur à 3 kills dont 3
headshots n'est PAS marqué tricheur.*

**ARN-3 — Top armes par région** → `<grp>.arena.top.weapons`
Compter les `KILL` par (`region`, `weapon`) sur **fenêtres hopping 15 min /
avance 5 min**. La `region` n'est pas dans l'événement : elle vient de la fiche
joueur via **GlobalKTable** sur `arena.players` — il faut donc enrichir
**avant** de grouper.
*Critères : sortie portant région + arme ; les comptes absents du catalogue
ressortent en région `INCONNU` au lieu de disparaître ; choix GlobalKTable vs
KTable justifié à l'oral.*

**ARN-4 — Chiffre d'affaires par joueur** → `<grp>.arena.revenue`
Cumuler `amount_eur` par `player_id` (KTable, `aggregate`). C'est ce qui part
en facturation.
*Critères : montants cohérents ; un doublon de `purchase_id` n'encaisse pas
deux fois (dédoublonnage à justifier) ; un achat rejeté en DLQ n'est pas
encaissé.*

**ARN-5 — Sessions de jeu** → `<grp>.arena.sessions`
**Fenêtres de session** (`SessionWindows`) : un trou de `SESSION_GAP_MINUTES`
sans le moindre événement ferme la session. Émettre `player_id`,
`session_start`, `session_end`, `duration_seconds`, `events`, et marquer les
sessions dépassant `MARATHON_MINUTES` (déjà câblés dans `TopologyProducer` —
gardez-les modifiables sans recompiler). En production le seuil marathon serait
à plusieurs heures ; il est court ici pour tenir dans une séance. Le générateur
lance un marathon toutes les ~30 min.
*Critères : chaque marathon est détecté ; une session normale (un match) fait
3 à 6 min ; **les retardataires ne doivent pas souder deux sessions** — un
événement qui arrive 90 min après les faits n'a rien à faire dans une fenêtre
de session.*

**ARN-6 — Résultat final de fenêtre**
En l'état, ARN-2 republie son agrégat **à chaque kill** : une fenêtre de 10 min
produit des dizaines de messages intermédiaires dont un seul est juste. Ne
publier que le **résultat final** de chaque fenêtre.
*Critères : le topic ARN-2 contient ~1 message par (joueur, fenêtre) et non
une dizaine ; savoir dire à l'oral ce que ça coûte (latence, mémoire) et
pourquoi la dernière fenêtre n'est jamais émise.*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Alimenter les topics d'entrée : le générateur, dans un second terminal,
#    depuis le dossier generateurs-v3/ du dépôt du cours (voir son README)
python generator.py --project arena --create-topics --replication-factor 1

# 3. Lancer en mode dev (live reload)
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
  (`arena-<groupe>`) préfixés — déjà câblé.
- Topics d'entrée partagés en lecture : **ne les modifiez pas**.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                   | Points |
|-----------------------------------------------------------|--------|
| Socle ARN-1 en production 10 min sans crash + DLQ motivée | **8**  |
| ARN-2 (aimbot + seuil de significativité)                 | +2     |
| ARN-3 (top armes + GlobalKTable)                          | +3     |
| ARN-4 (chiffre d'affaires + dédoublonnage)                | +3     |
| ARN-5 (fenêtres de session + marathon)                    | +2     |
| ARN-6 (résultat final de fenêtre)                         | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité.** Attendez-vous à
une demande de modification en direct (« Product Owner twist »).

## Conseils

- Le `Log.infof` fourni sert au premier contact : supprimez-le ensuite.
- Poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite le crash,
  la validation métier vous appartient.
- ARN-4 est le cœur métier : réfléchissez à la clé (`player_id`) et au
  dédoublonnage AVANT l'agrégation (un state store des `purchase_id` vus,
  avec une politique d'expiration, est une piste).
- ARN-5 est le morceau. `SessionWindows.ofInactivityGapWithNoGrace(...)` n'est
  pas `TimeWindows` : la fenêtre n'a pas de taille fixe, elle **grandit** et
  deux sessions **fusionnent** quand un événement arrive entre les deux.
  Regardez ce que produit votre agrégat quand cette fusion a lieu.
- ARN-5, l'autre moitié : le flux contient des événements vieux de 30 à
  180 min. Réfléchissez à ce qu'ils font à une fenêtre de session, et à ce que
  `grace` veut dire ici.
- ARN-6 : `suppress(...)` s'applique à une `KTable` fenêtrée, et il lui faut
  savoir quand la fenêtre est close.
- `split()`/`branch()` — l'ancienne API `branch(Predicate...)` a disparu en
  Kafka 4.0.
